import pandas as pd


weatherHead = pd.read_csv('hly175.csv')
weatherHead['date2'] = [dt.datetime.strptime(d, "%m/%d/%Y %H:%M") for d in weatherHead['date'] ]
weatherHead['date_for_merge'] = weatherHead['date2'].dt.round('H')
weatherHead = weatherHead[(weatherHead['date2'] >= '2020-04-01') & (weatherHead['date2'] < '2020-07-01')]
weatherHead =weatherHead.drop(['date_for_merge','date2'], axis=1)
print(weatherHead.head(2000).to_csv("hly175-head.csv"))
weatherHead = pd.read_csv('2020Q2.csv')
print(weatherHead.head(2000).to_csv('2020Q2-head.csv'))
