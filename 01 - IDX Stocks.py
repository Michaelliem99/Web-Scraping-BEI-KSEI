# Code History:
# 1. Version 1.0 (2023/03/09):
# - Base version, working as expected
# 2. Version 1.1 (2023/03/19):
# - Everytime **Financial Report Links** code runs, it won't overwrite any previous data, instead it will append new data for the previous scraped data.

# <strong>Features:</strong>
# - Scrape IDX individual stock summary and details
# - Scrape IDX sectoral stock summary and its components
# - Scrape IDX stock index summary
# 
# Plan: Data is scraped <strong>every weekday on 6PM GMT+7</strong>, few hours after the market has closed for the day. So the data you see before 6PM is previous trading day data.

import json
from json.decoder import JSONDecodeError
import pandas as pd
import numpy as np
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import threading
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import gc

import os
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

# # Chrome Selenium Starter
# 
# Why Selenium? Because I need it to bypass cloudfare restriction

# Initialize the Chrome driver

def driver_setup():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(options=options)

    return driver

print("Initialize Driver for Summary")
driver = driver_setup()

# # Scrape Summary URL

# ## URL List

urls = {
    'BEIStockSummary':'https://www.idx.co.id/primary/TradingSummary/GetStockSummary?length=9999&start=0',
    'BEISectoralSummary':'https://www.idx.co.id/primary/StockData/GetIndexIC',
    'BEIIndexSummary':'https://www.idx.co.id/primary/StockData/GetConstituent'
}

# ## BEI Stock Summary
print("Start Scrape Stock Summary")
while True:
    try:
        driver.get(urls['BEIStockSummary'])
        WebDriverWait(driver, timeout=10).until(lambda d: d.find_element(By.TAG_NAME, 'body'))
        BEIStockSummaryContent = driver.find_element(By.TAG_NAME, value='body').text
        BEIStockSummaryDF = pd.DataFrame(json.loads(BEIStockSummaryContent)['data']).drop(columns=['No'])
        BEIStockSummaryDF
        break
    except JSONDecodeError as e:
        time.sleep(1.5)
#             print(stock, 'Company Profiles JSON is not available!', 'Retrying!')

# ## Close and Quit Driver
driver.close()
driver.quit()
print("End Scrape Stock Summary")
# # Scrape Stock Details URL

# ## Company Profiles

def get_company_profiles(driver, stock):
    company_profiles_url = 'https://www.idx.co.id/primary/ListedCompany/GetCompanyProfilesDetail?KodeEmiten=' + stock
    while True:
        try:
            driver.get(company_profiles_url)
            WebDriverWait(driver, timeout=10).until(lambda d: d.find_element(By.TAG_NAME, 'body'))
            CompanyProfilesContent = driver.find_element(By.TAG_NAME, value='body').text
            CompanyProfilesRow = pd.DataFrame(json.loads(CompanyProfilesContent)['Profiles'])
            CompanyProfilesRow.insert(0, 'StockCode', stock)
            
            break
        except JSONDecodeError as e:
            time.sleep(1.5)
#             print(stock, 'Company Profiles JSON is not available!', 'Retrying!')
    
    time.sleep(0.75)    
    
    return CompanyProfilesRow

# ## Trading Info
today = datetime.today()
def get_trading_info(driver, engine, stock):
    global today
    query = '''
        SELECT \"IDXTradingInfo\".\"StockCode\", \"IDXTradingInfo\".\"Date\" FROM \"IDXTradingInfo\" 
        WHERE
            \"IDXTradingInfo\".\"StockCode\" = \'{}\' 
    '''.format(stock)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, con=conn)

    if len(df) > 0:
        max_date = df['Date'].max()
        difference = (today - max_date).days + 1

        trading_info_url = 'https://www.idx.co.id/primary/ListedCompany/GetTradingInfoSS?code={}&start=0&length={}'.format(stock, difference)
    else:
        trading_info_url = 'https://www.idx.co.id/primary/ListedCompany/GetTradingInfoSS?code={}&start=0&length=10000'.format(stock)

    while True:
        try:
            driver.get(trading_info_url)
            WebDriverWait(driver, timeout=10).until(lambda d: d.find_element(By.TAG_NAME, 'body'))
            TradingInfoContent = driver.find_element(By.TAG_NAME, value='body').text
            TradingInfoRows = pd.DataFrame(json.loads(TradingInfoContent)['replies'])
            
            break
        except JSONDecodeError as e:
            time.sleep(1.5)
#             print(stock, 'Trading Info JSON is not available!', 'Retrying!')
    
    time.sleep(0.75)
    
    return TradingInfoRows

# ## Financial Reports File Links
# Maximum last 3 years (Current: 2023, Min: 2021)
# 
# Code will only find for any missing data, previous available data won't be overwritten.

def get_financial_report_file_links(driver, engine, stock):
    current_year = datetime.now().year
    # last 3 years
    years = [current_year, current_year-2]
    periods = ['TW1', 'TW2', 'TW3', 'Audit']
    
    FinancialReportRows = pd.DataFrame()
    for year in years:
        for period in periods:
            query = '''
                SELECT * FROM \"IDXFinancialReportLinks\" 
                WHERE 
                    \"IDXFinancialReportLinks\".\"StockCode\" = \'{}\' 
                    AND \"IDXFinancialReportLinks\".\"Report_Period\" = \'{}\' 
                    AND \"IDXFinancialReportLinks\".\"Report_Year\" = \'{}\'
            '''.format(stock, period, year)

            with engine.connect() as conn:
                df = pd.read_sql(query, con=conn)
            
            if len(df) > 0:
                continue
            else:
                while True:
                    try:
                        financial_report_url = 'https://www.idx.co.id/primary/ListedCompany/GetFinancialReport?periode={}&year={}&indexFrom=0&pageSize=1000&reportType=rdf&kodeEmiten={}'.format(period, year, stock)
                        driver.get(financial_report_url)
                        WebDriverWait(driver, timeout=10).until(lambda d: d.find_element(By.TAG_NAME, 'body'))
                        FinancialReportContent = driver.find_element(By.TAG_NAME, value='body').text
                        if json.loads(FinancialReportContent)['ResultCount'] > 0:
                            FinancialReportRow = pd.DataFrame(json.loads(FinancialReportContent)['Results'][0]['Attachments'])
                            FinancialReportRow = FinancialReportRow.rename(columns={'Emiten_Code':'StockCode'})
                            FinancialReportRows = pd.concat([FinancialReportRows, FinancialReportRow])
                        break
                    except JSONDecodeError as e:
                        time.sleep(1.5)
    #                     print(stock, year, period, 'JSON is not available!', 'Retrying!')
        time.sleep(0.75)
    return FinancialReportRows

# ## Multithreading Scrape

# ### Driver Initialization

num_threads = 6

# ### Split StockID to Chunks

stock_list = BEIStockSummaryDF['StockCode'].to_list()
stock_chunks = np.array_split(stock_list, num_threads)

# ### Worker Function

# Define a worker function that takes stock codes from the queue and loads them in parallel

def load_stock(driver, engine, stock):
    company_profiles = get_company_profiles(driver, stock)
    trading_info = get_trading_info(driver, engine, stock)
    financial_report_links = get_financial_report_file_links(driver, engine, stock)
    
    return company_profiles, trading_info, financial_report_links

def load_stock_chunks(stock_list, total_stock_length, thread_id):
    global completion_count
    chunks_dict = {
        'CompanyProfiles':[],
        'TradingInfo':[],
        'FinancialReportLinks':[]
    }

    driver = driver_setup()
    engine = init_engine()

    for i, stock_code in enumerate(stock_list):
        company_profiles, trading_info, financial_report_links = load_stock(driver, engine, stock_code)

        chunks_dict['CompanyProfiles'].append(company_profiles)
        chunks_dict['TradingInfo'].append(trading_info)
        chunks_dict['FinancialReportLinks'].append(financial_report_links)

        completion_count += 1
        print(f"Thread {thread_id}: Stock {stock_code} processed, {i+1}/{len(stock_list)} in thread completed, {completion_count}/{total_stock_length} in total completed")

    driver.close()
    driver.quit()

    return chunks_dict

# ### Load SQL Engine for Previous Data

def init_engine():
    engine = create_engine(
        "postgresql://{}:{}@{}/{}".format(
            os.getenv('POSTGRE_USER'), os.getenv('POSTGRE_PW'), os.getenv('POSTGRE_HOST'), os.getenv('POSTGRE_DB')
        )
    )
    return engine


# ### Create list to store scraped data

results_dict = {
    'CompanyProfiles':[],
    'TradingInfo':[],
    'FinancialReportLinks':[]
}

# ### Run MultiThreading with Progress Status

print("Start Scrape Stock Details")

with ThreadPoolExecutor(max_workers=num_threads) as executor:
    futures = []
    completion_count = 0

    for i in range(num_threads):
        print(f'Thread {i+1} has {len(stock_chunks[i])} stocks')
        futures.append(executor.submit(load_stock_chunks, stock_chunks[i], len(stock_list), i+1))

    for future in as_completed(futures):
        chunks_dict = future.result()

        results_dict['CompanyProfiles'].extend(chunks_dict['CompanyProfiles'])
        results_dict['TradingInfo'].extend(chunks_dict['TradingInfo'])
        results_dict['FinancialReportLinks'].extend(chunks_dict['FinancialReportLinks'])

print("End Scrape Stock Details")

# ## Join All Stock Details and Export Result
print("Data Transformation and Export Result")

# ### Export SQL
def export_sql(df, name, if_exists='replace'):
    engine = create_engine(
        "postgresql://{}:{}@{}/{}".format(
            os.getenv('POSTGRE_USER'), os.getenv('POSTGRE_PW'), os.getenv('POSTGRE_HOST'), os.getenv('POSTGRE_DB')
        )
    )
    conn = engine.connect()

    df.to_sql(name, con=conn, if_exists=if_exists, index=False)

# ### Data Transformation

CompanyProfilesDF = pd.concat(results_dict['CompanyProfiles']).reset_index(drop=True).drop(
    columns=[
        'DataID', 'Divisi', 'EfekEmiten_EBA', 'EfekEmiten_ETF', 
        'EfekEmiten_Obligasi', 'EfekEmiten_SPEI', 'EfekEmiten_Saham',
        'id', 'KodeDivisi', 'JenisEmiten', 'KodeEmiten', 'Status'
    ]
)
CompanyProfilesDF['TanggalPencatatan'] = pd.to_datetime(CompanyProfilesDF['TanggalPencatatan']).dt.normalize()
CompanyProfilesDF['Logo'] = ['https://www.idx.co.id' + logo for logo in CompanyProfilesDF['Logo']]
CompanyProfilesDF['LastScraped'] = datetime.now()

export_sql(CompanyProfilesDF, 'IDXCompanyProfiles')
del CompanyProfilesDF
results_dict['CompanyProfiles'] = None
gc.collect()

TradingInfoDF = pd.concat(results_dict['TradingInfo']).drop(columns=['No', 'Remarks']).reset_index(drop=True)
TradingInfoDF['Date'] = pd.to_datetime(TradingInfoDF['Date'])
TradingInfoDF['LastScraped'] = datetime.now()
TradingInfoDF

export_sql(TradingInfoDF, 'IDXTradingInfo', if_exists='append')
del TradingInfoDF
results_dict['TradingInfo'] = None
gc.collect()

FinancialReportLinksDF = pd.concat(results_dict['FinancialReportLinks']).reset_index(drop=True).drop(
    columns=['File_ID', 'File_Size', 'File_Type']
)
FinancialReportLinksDF['File_Modified'] = pd.to_datetime(FinancialReportLinksDF['File_Modified']).dt.normalize()
FinancialReportLinksDF['File_Path'] = 'https://www.idx.co.id/' + FinancialReportLinksDF['File_Path']
FinancialReportLinksDF['LastScraped'] = datetime.now()
FinancialReportLinksDF

export_sql(FinancialReportLinksDF, 'IDXFinancialReportLinks', if_exists='append')
del FinancialReportLinksDF, results_dict
gc.collect()

# # Export Result

# ## Export to Excel

# with pd.ExcelWriter('stocks.xlsx') as writer:
#     CompanyProfilesDF.to_excel(writer, sheet_name='Company Profiles', index=False)
#     TradingInfoDF.to_excel(writer, sheet_name='Trading Info', index=False)
#     FinancialReportLinksDF.to_excel(writer, sheet_name='Financial Reports', index=False)

# ## Export to DB

