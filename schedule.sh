#!/bin/sh
cd /home/michaelliem99/Desktop/Portfolio/Web-Scraping-BEI-KSEI
now=$(date)
echo "Start Time: $now" &&
echo "START 01- IDX STOCKS" &&
/home/michaelliem99/anaconda3/envs/web_scraping/bin/python "/home/michaelliem99/Desktop/Portfolio/Web-Scraping-BEI-KSEI/01 - IDX Stocks.py" &&
echo "START 02 - IDX STOCK SECTORAL AND INDEX" &&
/home/michaelliem99/anaconda3/envs/web_scraping/bin/python "/home/michaelliem99/Desktop/Portfolio/Web-Scraping-BEI-KSEI/02 - IDX Stock Sectoral and Index.py" &&
echo "START 03 - IDX KSEI BONDS" &&
/home/michaelliem99/anaconda3/envs/web_scraping/bin/python "/home/michaelliem99/Desktop/Portfolio/Web-Scraping-BEI-KSEI/03 - KSEI Bonds.py" &&
now=$(date)
echo "End Time: $now" && 
rm -r /tmp/.com.google.Chrome*
