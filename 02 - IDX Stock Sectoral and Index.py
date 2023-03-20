# Code History:
# 1. Version 1.0 (2023/03/09):
# - Base version, working as expected

# <strong>Features:</strong>
# - Scrape IDX stock sectoral summary
# - Scrape IDX stock index summary
# 
# Plan: Data is scraped <strong>every weekday on 6PM GMT+7</strong>, few hours after the market has closed for the day. So the data you see before 6PM is previous trading day data.

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
from tqdm import tqdm
import os
import sqlalchemy
from sqlalchemy import create_engine

# # Chrome Selenium Starter
# 
# Why Selenium? Because I need it to bypass cloudfare restriction

# Initialize the Chrome driver

options = Options()
options.add_argument("--no-sandbox")
options.add_argument("--headless=new")
driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

# # Scrape Summary URL

# ## URL List

urls = {
    'BEISectoralSummary':'https://www.idx.co.id/primary/StockData/GetIndexIC',
    'BEIIndexSummary':'https://www.idx.co.id/primary/StockData/GetConstituent',
}

# ## BEI Sectoral Summary

driver.get(urls['BEISectoralSummary'])
WebDriverWait(driver, timeout=10).until(lambda d: d.find_element(By.TAG_NAME, 'body'))
BEISectoralSummaryContent = driver.find_element(By.TAG_NAME, value='body').text
time.sleep(2)

while True:
    try:
        BEISectoralSummaryDF = pd.DataFrame(json.loads(BEISectoralSummaryContent)['data']).drop(columns='IntRow')
        BEISectoralSummaryDF['DTCreate'] = pd.to_datetime(BEISectoralSummaryDF['DTCreate']).dt.normalize()
        BEISectoralSummaryDF['LastScraped'] = datetime.now()
        BEISectoralSummaryDF
        break
    except JSONDecodeError as e:
        time.sleep(1.5)

PrevSectoralSummary = pd.read_excel('stock_index_sectoral.xlsx', sheet_name='Sectoral Summary')
BEISectoralSummaryDF = pd.concat(
    [BEISectoralSummaryDF, PrevSectoralSummary]
).sort_values(
    by=['DTCreate', 'LastScraped']
).drop_duplicates(
    subset=['IndexCode', 'DTCreate'],
    keep='first'
)
BEISectoralSummaryDF

# ## BEI Index Summary

driver.get(urls['BEIIndexSummary'])
WebDriverWait(driver, timeout=10).until(lambda d: d.find_element(By.TAG_NAME, 'body'))
BEIIndexSummaryContent = driver.find_element(By.TAG_NAME, value='body').text
time.sleep(2)

while True:
    try:
        BEIIndexSummaryDF = pd.DataFrame(json.loads(BEIIndexSummaryContent)['Items']).drop(columns='Links')
        BEIIndexSummaryDF['DtCreate'] = pd.to_datetime(BEIIndexSummaryDF['DtCreate']).dt.normalize()
        BEIIndexSummaryDF = BEIIndexSummaryDF.rename(columns={'DtCreate':'DTCreate'})
        BEIIndexSummaryDF['LastScraped'] = datetime.now()
        BEIIndexSummaryDF
        break
    except JSONDecodeError as e:
        time.sleep(1.5)

PrevIndexSummary = pd.read_excel('stock_index_sectoral.xlsx', sheet_name='Index Summary')
BEIIndexSummaryDF = pd.concat(
    [BEIIndexSummaryDF, PrevIndexSummary]
).sort_values(
    by=['DTCreate', 'LastScraped']
).drop_duplicates(
    subset=['IndexCode', 'DTCreate'],
    keep='first'
)
BEIIndexSummaryDF

# ## Close and Quit Driver

driver.quit()

# # Export Result

# ## Export to Excel

# with pd.ExcelWriter('stock_index_sectoral.xlsx') as writer:
#     BEISectoralSummaryDF.to_excel(writer, sheet_name='Sectoral Summary', index=False)
#     BEIIndexSummaryDF.to_excel(writer, sheet_name='Index Summary', index=False)

# ## Export to DB

engine = create_engine(
    "postgresql://{}:{}@{}/{}".format(
        os.getenv('POSTGRE_USER'), os.getenv('POSTGRE_PW'), os.getenv('POSTGRE_HOST'), os.getenv('POSTGRE_DB')
    )
)
conn = engine.connect()

BEISectoralSummaryDF.to_sql('BEISectoralSummary', con=conn, if_exists='replace', index=False)
BEIIndexSummaryDF.to_sql('BEIIndexSummary', con=conn, if_exists='replace', index=False)