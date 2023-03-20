#!/bin/sh
now=$(date)
echo "Start Time: $now" &&
echo "Start IDX Stocks" &&
/home/michaelliem99/anaconda3/envs/web_scraping/bin/python "01 - IDX Stocks.py" &&
echo "Start IDX Stock Sectoral and Index" &&
/home/michaelliem99/anaconda3/envs/web_scraping/bin/python "02 - IDX Stock Sectoral and Index.py" &&
echo "Start IDX KSEI Bonds" &&
/home/michaelliem99/anaconda3/envs/web_scraping/bin/python "03 - KSEI Bonds.py" &&
now=$(date)
echo "End Time: $now"