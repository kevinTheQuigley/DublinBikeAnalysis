#! /usr/bin/python3
# ReduceDailyStationUse.py
# Joining the bikes and the Weather datasets
import datetime as dt
import pandas as pd
from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol


class ReduceLeftJoinEx(MRJob):

    OUTPUT_PROTOCOL = JSONValueProtocol # The reducer will output only the value, not the key

    # Allow the job to receive an argument from the command line
    # This argument will be used to specify the type of join

    def mapper(self, _, line):
        line=line.strip()
        fields = line.split(',')
        date_for_merge = pd.to_datetime(dt.datetime.strptime(fields[0][2:-1],"%Y/%m/%d"))
        date_for_value = date_for_merge.strftime("%Y/%m/%d")
        stationName = fields[1][2:-1]
        rainyDay    = fields[2][2:-1] # These could be swapped <>
        totalBikeUse= fields[3]
        rainfall    = fields[4]
        avgTemp     = fields[5]
        stationID   = fields[7]
        value = (date_for_value,stationName,rainyDay,totalBikeUse,rainfall,avgTemp,stationID)
        key         = str(int(stationID) + 1000*(1+int(rainyDay)))
        yield key, value

    def reducer(self, key, values):
        total = 0
        count = 0
        finishedStations =[]
        weather_tuples = []
        bike_tuples = []
        bikeDelta = 0
        oldBikeAvailibility  = -1 
        totalWet = 0
        totalTemp = 0
        totalBikeChange = 0
        i = 0
        totalRain = 0
        for value in list(values):
            i+=1
            date = value[0]
            name = value[1]
            wet  = int(value[2])
            totalBikeUse  = float(value[3])
            rain = float(value[4])
            stationID = int(value[6])
            totalBikeChange += abs(totalBikeUse)
            totalRain += rain
            totalWet = int(wet)
        avgBikeUse = totalBikeChange/i 
        value = (key,name,wet,avgBikeUse,totalRain/i,stationID,str(i))
        yield(key, value)


if __name__ == '__main__':
    ReduceLeftJoinEx.run()
