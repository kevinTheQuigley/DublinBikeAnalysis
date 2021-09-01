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
        date_for_merge = pd.to_datetime(dt.datetime.strptime(fields[0][2:16],"%Y/%m/%d %H:")).floor('h') #Had to change these due to space removal
        date_for_value = date_for_merge.strftime("%Y/%m/%d")
        stationID   = fields[1][2:-1]
        stationName = fields[2][2:-1]
        bikeStands  = fields[3][2:-1]
        availableBikes= fields[4][2:-1]
        address     = fields[5][2:-1]
        lattitude   = fields[6][2:-1]
        longitude   = fields[7][2:-1]
        occupancy   = fields[8][1:]
        full        = fields[9][2:-1]
        empty       = fields[10][2:-1]
        dayNumber   = fields[11][2:-1]
        dayType     = fields[12][2:-1]
        rainfall    = fields[13][2:-1]
        temp        = fields[14][2:-1]
        wet         = fields[15][1:] # These could be swapped <>
        warm        = fields[16][1:-1]
        hour        = date_for_merge.hour
        value = (hour,rainfall,temp,stationName,wet,warm,availableBikes,date_for_value,stationID)
        key         = int(stationID) + 100*int(date_for_merge.strftime("%Y%m%d"))
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
            hour = value[0]
            rain = float(value[1])
            temp = float(value[2])
            name = value[3]
            wet  = value[4]
            warm = value[5]
            key  = value[7]
            stationID = value[8]
            availableBikes  = int(value[6])
            if  i  == 0:
                bikeDelta = 0
            else:
                bikeDelta = oldBikeAvailability - availableBikes
            i += 1
            totalBikeChange += abs(bikeDelta)
            totalRain += rain
            oldBikeAvailability = availableBikes
            totalWet += int(wet)
            totalTemp += temp
        value = (key,name,str(int(bool(totalWet))),totalBikeChange,totalRain,(totalTemp/i),warm,stationID,str(i))
        yield(key, value)


if __name__ == '__main__':
    ReduceLeftJoinEx.run()
