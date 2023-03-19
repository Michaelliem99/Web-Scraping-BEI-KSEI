#!/usr/bin/env python
# coding: utf-8

# Code History:
# 1. Version 1.0 (2023/03/09):
#     - Base version, working as expected

# <strong>Features:</strong>
# - Scrape corporate and government bonds summary and details
# 
# Plan: Data is scraped <strong>every weekday on 6PM GMT+7</strong>, few hours after the market has closed for the day. So the data you see before 6PM is previous trading day data.

# In[1]:


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


# # Chrome Selenium Starter
# 
# Why Selenium? Because I need it to bypass cloudfare restriction

# In[2]:


# Initialize the Chrome driver
options = Options()
options.add_argument("--headless=new")
driver = webdriver.Chrome(options=options)


# # Scrape Bond Summary

# ## BEI Bonds List

# In[3]:


urls = {
    'Corporate Bond':'https://www.idx.co.id/secondary/get/BondSukuk/bond?pageSize=10000&indexFrom=1&bondType=1',
    'Goverment Bond':'https://www.idx.co.id/secondary/get/BondSukuk/bond?pageSize=10000&indexFrom=1&bondType=2'  
}


# In[4]:


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


# In[5]:


BEIBondsListDF


# ## Close and Quit Driver

# In[6]:


driver.quit()


# # Scrape Bond Details

# ## Get Bond Details Function

# In[7]:


## Well, the website has a weird issue, i can access medium term notes with url intended for corporate / govt bonds
## MTN example: https://www.ksei.co.id/services/registered-securities/medium-term-notes/lc/ABLS01XXMF
## Different URL example: https://www.ksei.co.id/services/registered-securities/corporate-bonds/lc/ABLS01XXMF
## Try it and you can still access the medium term notes
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
    
    time.sleep(2)

    return data


# ## Multithreading with Progress Bar

# In[8]:


df_list = []

with tqdm(total=len(BEIBondsListDF['BondId'])) as pbar:
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        
        for BondId in BEIBondsListDF['BondId']:
            future = executor.submit(get_bond_details, BondId)
            futures.append(future)

        # Use tqdm to add a progress bar to the multithreading process
        for future in as_completed(futures):
            pbar.update(1)
            df_list.append(future.result())


# ## Join All Bond Details and Cleaning

# ### Join Bond Details

# In[9]:


BondDetailsDF = pd.DataFrame(df_list)
BondDetailsDF


# In[10]:


BondDetailsDF.columns


# ### Data Transformation
# 
# 1. Some dates are written in Indonesian format string, 'May' is written as 'Mei', so I use dateparser library to tackle this issue and convert it to pandas datetime column
# 2. Interest rate format is string, convert it to float32
# 3. Replace '-' string with NaN

# In[11]:


BondDetailsDF['Listing Date'] = BondDetailsDF['Listing Date'].apply(lambda x: dateparser.parse(x) if x != '-' else np.nan)
BondDetailsDF['Mature Date'] = BondDetailsDF['Mature Date'].apply(lambda x: dateparser.parse(x) if ((x != '-') and (type(x) == str)) else np.nan)
BondDetailsDF['Effective Date ISIN'] = BondDetailsDF['Effective Date ISIN'].apply(lambda x: dateparser.parse(x) if x != '-' else np.nan)
BondDetailsDF['Interest/Disc Rate'] = BondDetailsDF['Interest/Disc Rate'].replace('%', '', regex=True).apply('float32')
BondDetailsDF = BondDetailsDF.replace('-', np.nan)


# In[12]:


BondDetailsDF.describe(include='all')


# ### Drop Unnecessary Columns
# 
# 1. Every column dropped has mostly missing value

# In[13]:


BondDetailsDF = BondDetailsDF.drop(columns=['Current Amount', 'Effective Date ISIN', 'Day Count Basis', 'Exercise Price'])


# ## Export to Excel

# In[16]:


BondDetailsDF['LastScraped'] = datetime.now()
BondDetailsDF


# In[15]:


BondDetailsDF.to_excel('bonds.xlsx', index=False)


# In[ ]:



