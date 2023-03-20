#!/bin/sh
now=$(date)
echo "Start Time: $now"
/root/anaconda3/envs/web_scraping/bin/python "01 - IDX Stocks.py"
/root/anaconda3/envs/web_scraping/bin/python "02 - IDX Stock Sectoral and Index.py"
/root/anaconda3/envs/web_scraping/bin/python "03 - KSEI Bonds.py"
now=$(date)
echo "End Time: $now"