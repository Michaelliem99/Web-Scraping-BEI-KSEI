#!/usr/bin/env python
# coding: utf-8

# Code History:
# 1. Version 1.0 (2023/03/09):
#     - Base version, working as expected
# 2. Version 1.1 (2023/03/19):
#     - Everytime **Financial Report Links** code runs, it won't overwrite any previous data, instead it will append new data for the previous scraped data.

# <strong>Features:</strong>
# - Scrape IDX individual stock summary and details
# - Scrape IDX sectoral stock summary and its components
# - Scrape IDX stock index summary
# 
# Plan: Data is scraped <strong>every weekday on 6PM GMT+7</strong>, few hours after the market has closed for the day. So the data you see before 6PM is previous trading day data.

# In[1]:


import json
from json.decoder import JSONDecodeError
import pandas as pd
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


# # Chrome Selenium Starter
# 
# Why Selenium? Because I need it to bypass cloudfare restriction

# In[2]:


# Initialize the Chrome driver

options = Options()
options.add_argument("--headless=new")
driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

# # Scrape Summary URL

# ## URL List

# In[3]:


urls = {
    'BEIStockSummary':'https://www.idx.co.id/primary/TradingSummary/GetStockSummary?length=9999&start=0',
    'BEISectoralSummary':'https://www.idx.co.id/primary/StockData/GetIndexIC',
    'BEIIndexSummary':'https://www.idx.co.id/primary/StockData/GetConstituent'
}


# ## BEI Stock Summary

# In[4]:
while True:
    try:
        driver.get(urls['BEIStockSummary'])
        WebDriverWait(driver, timeout=10).until(lambda d: d.find_element(By.TAG_NAME, 'body'))
        BEIStockSummaryContent = driver.find_element(By.TAG_NAME, value='body').text
        break
    except JSONDecodeError as e:
        time.sleep(1.5)
#             print(stock, 'Company Profiles JSON is not available!', 'Retrying!')

# In[5]:

BEIStockSummaryDF = pd.DataFrame(json.loads(BEIStockSummaryContent)['data']).drop(columns=['No'])
BEIStockSummaryDF

# ## Close and Quit Driver

# In[6]:

driver.quit()


# # Scrape Stock Details URL

# ## Company Profiles

# In[7]:

def get_company_profiles(driver, stock):
    while True:
        try:
            company_profiles_url = 'https://www.idx.co.id/primary/ListedCompany/GetCompanyProfilesDetail?KodeEmiten=' + stock
            driver.get(company_profiles_url)

            WebDriverWait(driver, timeout=10).until(lambda d: d.find_element(By.TAG_NAME, 'body'))

            CompanyProfilesContent = driver.find_element(By.TAG_NAME, value='body').text
            CompanyProfilesRow = pd.DataFrame(json.loads(CompanyProfilesContent)['Profiles'])
            CompanyProfilesRow.insert(0, 'StockCode', stock)
            
            break
        except JSONDecodeError as e:
            time.sleep(1.5)
#             print(stock, 'Company Profiles JSON is not available!', 'Retrying!')
    
    time.sleep(1)    
    
    return CompanyProfilesRow


# ## Today Trading Info

# In[8]:


def get_today_trading_info(driver, stock):
    while True:
        try:
            trading_info_url = 'https://www.idx.co.id/primary/ListedCompany/GetTradingInfoSS?code={}&start=0&length=10000'.format(stock)
            driver.get(trading_info_url)

            WebDriverWait(driver, timeout=10).until(lambda d: d.find_element(By.TAG_NAME, 'body'))

            TradingInfoContent = driver.find_element(By.TAG_NAME, value='body').text
            TradingInfoRows = pd.DataFrame(json.loads(TradingInfoContent)['replies'])
            
            break
        except JSONDecodeError as e:
            time.sleep(1.5)
#             print(stock, 'Trading Info JSON is not available!', 'Retrying!')
    
    time.sleep(1)
    
    return TradingInfoRows


# ## Financial Reports File Links
# Maximum last 2 years (Current: 2023, Min: 2022)
# 
# Code will only find for any missing data, previous available data won't be overwritten.

# In[9]:


def get_financial_report_file_links(driver, stock, prev_financial_report):
    current_year = datetime.now().year
    # last 2 years
    years = [current_year, current_year-1]
    periods = ['TW1', 'TW2', 'TW3', 'Audit']
    
    FinancialReportRows = pd.DataFrame()

    for year in years:
        for period in periods:
            if len(prev_financial_report_stock[(prev_financial_report_stock['Report_Period'] == period) & (prev_financial_report_stock['Report_Year'] == year)]) > 0:
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

        time.sleep(2)

    return FinancialReportRows


# ## Multithreading Scrape

# ### Worker Function

# In[10]:


# Define a worker function that takes stock codes from the queue and loads them in parallel
def load_stock(stock, prev_financial_report_stock):
    options = Options()
    options.add_argument("--headless=new")
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

    company_profiles = get_company_profiles(driver, stock)
    today_trading_info = get_today_trading_info(driver, stock)
    financial_report_links = get_financial_report_file_links(driver, stock, prev_financial_report_stock)
    
    return company_profiles, today_trading_info, financial_report_links


# ### Create list to store scraped data

# In[11]:


prev_financial_report_df = pd.read_excel('stocks.xlsx', sheet_name='Financial Reports')


# In[12]:


results = {
    'CompanyProfiles':[],
    'TodayTradingInfo':[],
    'FinancialReportLinks':[]
}


# In[13]:


prev_financial_report_df = pd.read_excel('stocks.xlsx', sheet_name='Financial Reports')

with tqdm(total=len(BEIStockSummaryDF['StockCode'])) as pbar:
    with ThreadPoolExecutor(max_workers=7) as executor:
        futures = []

        for StockCode in BEIStockSummaryDF['StockCode']:
            prev_financial_report_stock = prev_financial_report_df[prev_financial_report_df['StockCode'] == StockCode]
            future = executor.submit(load_stock, StockCode, prev_financial_report_stock)
            futures.append(future)
            
        for future in as_completed(futures):
            pbar.update(1)
            company_profiles, today_trading_info, financial_report_links = future.result()
            results['CompanyProfiles'].append(company_profiles)
            results['TodayTradingInfo'].append(today_trading_info)
            results['FinancialReportLinks'].append(financial_report_links)


# ## Join All Stock Details

# In[14]:


CompanyProfilesDF = pd.concat(results['CompanyProfiles']).reset_index(drop=True).drop(
    columns=[
        'DataID', 'Divisi', 'EfekEmiten_EBA', 'EfekEmiten_ETF', 
        'EfekEmiten_Obligasi', 'EfekEmiten_SPEI', 'EfekEmiten_Saham',
        'id', 'KodeDivisi', 'JenisEmiten', 'KodeEmiten', 'Status'
    ]
)
CompanyProfilesDF['TanggalPencatatan'] = pd.to_datetime(CompanyProfilesDF['TanggalPencatatan']).dt.normalize()
CompanyProfilesDF['Logo'] = ['https://www.idx.co.id' + logo for logo in CompanyProfilesDF['Logo']]
CompanyProfilesDF['LastScraped'] = datetime.now()
CompanyProfilesDF


# In[15]:


prev_trading_info = pd.read_excel('stocks.xlsx', sheet_name='Trading Info')
TradingInfoDF = pd.concat(results['TodayTradingInfo']).drop(columns=['No', 'Remarks']).reset_index(drop=True)
TradingInfoDF['Date'] = pd.to_datetime(TradingInfoDF['Date'])
TradingInfoDF['LastScraped'] = datetime.now()
TradingInfoDF = pd.concat([TradingInfoDF, prev_trading_info]).sort_values(by='Date').drop_duplicates(subset=['StockCode', 'Date'], keep='first').reset_index(drop=True)
TradingInfoDF


# In[16]:


FinancialReportLinksDF = pd.concat(results['FinancialReportLinks']).reset_index(drop=True).drop(
    columns=['File_ID', 'File_Size', 'File_Type']
)
FinancialReportLinksDF['File_Modified'] = pd.to_datetime(FinancialReportLinksDF['File_Modified']).dt.normalize()
FinancialReportLinksDF['File_Path'] = 'https://www.idx.co.id/' + FinancialReportLinksDF['File_Path']
FinancialReportLinksDF['LastScraped'] = datetime.now()
FinancialReportLinksDF = pd.concat([FinancialReportLinksDF, prev_financial_report_df]).reset_index(drop=True)
FinancialReportLinksDF


# ## Append Previous Trading Day Data

# ## Export to Excel

# In[18]:


with pd.ExcelWriter('stocks.xlsx') as writer:
    CompanyProfilesDF.to_excel(writer, sheet_name='Company Profiles', index=False)
    TradingInfoDF.to_excel(writer, sheet_name='Trading Info', index=False)
    FinancialReportLinksDF.to_excel(writer, sheet_name='Financial Reports', index=False)


# In[ ]:




