#!/bin/sh
cd /home/michaelliem99/Desktop/Portfolio/Web-Scraping-BEI-KSEI
now=$(date)
echo "Start Time: $now" &&
echo "Start IDX Stocks" &&
/home/michaelliem99/anaconda3/envs/web_scraping/bin/python "/home/michaelliem99/Desktop/Portfolio/Web-Scraping-BEI-KSEI/01 - IDX Stocks.py" &&
echo "Start IDX Stock Sectoral and Index" &&
/home/michaelliem99/anaconda3/envs/web_scraping/bin/python "/home/michaelliem99/Desktop/Portfolio/Web-Scraping-BEI-KSEI/02 - IDX Stock Sectoral and Index.py" &&
echo "Start IDX KSEI Bonds" &&
/home/michaelliem99/anaconda3/envs/web_scraping/bin/python "/home/michaelliem99/Desktop/Portfolio/Web-Scraping-BEI-KSEI/03 - KSEI Bonds.py" &&
now=$(date)
echo "End Time: $now"
