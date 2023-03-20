# Code History:
# 1. Version 1.0 (2023/03/09):
# - Base version, working as expected

# <strong>Features:</strong>
# - Scrape corporate and government bonds summary and details
# 
# Plan: Data is scraped <strong>every weekday on 6PM GMT+7</strong>, few hours after the market has closed for the day. So the data you see before 6PM is previous trading day data.

import json
from json.decoder import JSONDecodeError
import numpy as np
import pandas as pd
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import queue
import threading
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import dateparser

import os
import sqlalchemy
from sqlalchemy import create_engine

# # Chrome Selenium Starter
# 
# Why Selenium? Because I need it to bypass cloudfare restriction

# Initialize the Chrome driver
print("Start Initialize Chrome Driver!")
options = Options()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
driver = webdriver.Chrome(options=options)
print("Initialize Chrome Driver Done!")
# # Scrape Bond Summary

# ## BEI Bonds List
print("Start Scrape Bond Summary")
urls = {
    'Corporate Bond':'https://www.idx.co.id/secondary/get/BondSukuk/bond?pageSize=10000&indexFrom=1&bondType=1',
    'Goverment Bond':'https://www.idx.co.id/secondary/get/BondSukuk/bond?pageSize=10000&indexFrom=1&bondType=2'  
}

BEIBondsListDF = pd.DataFrame()
for issuer_type in urls:
    print(issuer_type)
    driver.get(urls[issuer_type])
    WebDriverWait(driver, timeout=10).until(lambda d: d.find_element(By.TAG_NAME, 'body'))
    BEIBondsListContent = driver.find_element(By.TAG_NAME, value='body').text
    BEIBondsTypeListDF = pd.DataFrame(json.loads(BEIBondsListContent)['Results']).drop(columns='Nomor')
    BEIBondsTypeListDF['IssuerType'] = issuer_type
    
    BEIBondsListDF = pd.concat([BEIBondsListDF, BEIBondsTypeListDF])
    
BEIBondsListDF['MatureDate'] = pd.to_datetime(BEIBondsListDF['MatureDate']).dt.normalize()

BEIBondsListDF

# ## Close and Quit Driver

driver.quit()
print("End Scrape Bond Summary")

# # Scrape Bond Details

# ## Get Bond Details Function

# Well, the website has a weird issue, i can access medium term notes with url intended for corporate / govt bonds
# MTN example: https://www.ksei.co.id/services/registered-securities/medium-term-notes/lc/ABLS01XXMF
# Different URL example: https://www.ksei.co.id/services/registered-securities/corporate-bonds/lc/ABLS01XXMF
# Try it and you can still access the medium term notes
# 'https://www.ksei.co.id/services/registered-securities/medium-term-notes/lc/ABLS01XXMF'
# 'https://www.ksei.co.id/services/registered-securities/government-bonds/lc/FR0037'

def get_bond_details(BondId):
    while True:
        try:
            url = 'https://www.ksei.co.id/services/registered-securities/corporate-bonds/lc/' + BondId
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            data = {}

            # Find the dl tag with class="deflist deflist--with-colon"
            dl_tag = soup.find('dl', class_='deflist deflist--with-colon')

            # Loop through all dt tags within the dl tag and get their text values
            dt_tags = dl_tag.find_all('dt')
            for dt in dt_tags:
                # Get the text value of the dt tag
                dt_text = dt.get_text(strip=True)
                # Get the corresponding dd tag and its text value
                # find_next_sibling is actually an important function and it's new for me xD
                dd_text = dt.find_next_sibling('dd').get_text(strip=True)
                # Add the dd_text to the data dictionary with the dt_text as the key
                data[dt_text] = dd_text
            break
        except:
            time.sleep(1.5)
    
    time.sleep(1)
    return data

# ## Multithreading with Progress Bar

# ## Load Previous Scraped Data

engine = create_engine(
    "postgresql://{}:{}@{}/{}".format(
        os.getenv('POSTGRE_USER'), os.getenv('POSTGRE_PW'), os.getenv('POSTGRE_HOST'), os.getenv('POSTGRE_DB')
    )
)
conn = engine.connect()

prev_bond_details_df = pd.read_sql('SELECT * FROM BondDetails', con=conn)

# ## Create List to Store Scraped Data

df_list = []

print("Start Scrape Bond Details")
with tqdm(total=len(BEIBondsListDF['BondId'])) as pbar:
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = []
        
        for BondId in BEIBondsListDF['BondId']:
            if BondId in prev_bond_details_df['Short Code']:
                continue
            else:
                future = executor.submit(get_bond_details, BondId)
                futures.append(future)

        # Use tqdm to add a progress bar to the multithreading process
        for future in as_completed(futures):
            pbar.update(1)
            df_list.append(future.result())

print("End Scrape Bond Details")
# ## Join All Bond Details and Cleaning

# ### Join Bond Details

BondDetailsDF = pd.DataFrame(df_list)
BondDetailsDF

BondDetailsDF.columns

# ### Data Transformation
# 
# 1. Some dates are written in Indonesian format string, 'May' is written as 'Mei', so I use dateparser library to tackle this issue and convert it to pandas datetime column
# 2. Interest rate format is string, convert it to float32
# 3. Replace '-' string with NaN

print("Data Transformation")
BondDetailsDF['Listing Date'] = BondDetailsDF['Listing Date'].apply(lambda x: dateparser.parse(x) if x != '-' else np.nan)
BondDetailsDF['Mature Date'] = BondDetailsDF['Mature Date'].apply(lambda x: dateparser.parse(x) if ((x != '-') and (type(x) == str)) else np.nan)
BondDetailsDF['Effective Date ISIN'] = BondDetailsDF['Effective Date ISIN'].apply(lambda x: dateparser.parse(x) if x != '-' else np.nan)
BondDetailsDF['Interest/Disc Rate'] = BondDetailsDF['Interest/Disc Rate'].replace('%', '', regex=True).apply('float32')
BondDetailsDF = BondDetailsDF.replace('-', np.nan)

BondDetailsDF.describe(include='all')

# ### Drop Unnecessary Columns
# 
# 1. Every column dropped has mostly missing value

BondDetailsDF = BondDetailsDF.drop(columns=['Current Amount', 'Effective Date ISIN', 'Day Count Basis'])

# # Export Result
print("Export Result")
BondDetailsDF['LastScraped'] = datetime.now()
BondDetailsDF = pd.concat([prev_bond_details_df, BondDetailsDF])
BondDetailsDF

# ## Export to Excel

# BondDetailsDF.to_excel('bonds.xlsx', index=False)

# ## Export to DB

BondDetailsDF.to_sql('BondDetails', con=conn, if_exists='replace', index=False)