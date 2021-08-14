#!/bin/bash
curl –s https://data.smartdublin.ie/dataset/33ec9fe2-4957-4e9a-ab55-c5e917c7a9ab/resource/5d23332e-4f49-4c41-b6a0-bffb77b33d64/download/dublinbikes_20191001_20200101.csv > 2019Q4.csv --silent
curl –s  https://data.smartdublin.ie/dataset/33ec9fe2-4957-4e9a-ab55-c5e917c7a9ab/resource/aab12e7d-547f-463a-86b1-e22002884587/download/dublinbikes_20200101_20200401.csv > 2020Q1.csv --silent
curl –os 2019Q1.csv https://data.smartdublin.ie/dataset/33ec9fe2-4957-4e9a-ab55-c5e917c7a9ab/resource/8ddaeac6-4caf-4289-9835-cf588d0b69e5/download/dublinbikes_20200401_20200701.csv > 2020Q2.csv --silent
curl –os 2019Q1.csv https://data.smartdublin.ie/dataset/33ec9fe2-4957-4e9a-ab55-c5e917c7a9ab/resource/99a35442-6878-4c2d-8dff-ec43e91d21d7/download/dublinbikes_20200701_20201001.csv > 2020Q3.csv --silent
curl –os 2019Q1.csv https://data.smartdublin.ie/dataset/33ec9fe2-4957-4e9a-ab55-c5e917c7a9ab/resource/5328239f-bcc6-483d-9c17-87166efc3a1a/download/dublinbikes_20201001_20210101.csv > 2020Q4.csv --silent
wget https://cli.fusio.net/cli/climate_data/webdata/hly175.zip
unzip hly175.zip
rm -rf hly175.zip Data_Licence.txt KeyHourly.txt 
curl –s https://opendata-geohive.hub.arcgis.com/datasets/d8eb52d56273413b84b0187a4e9117be_0.csv?outSR=%7B%22latestWkid%22%3A3857%2C%22wkid%22%3A102100%7D > Covid.csv --silent
