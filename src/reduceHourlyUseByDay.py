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
        dayNumber   = fields[11][1:]
        dayType     = fields[12][2:-1]
        rainfall    = fields[13][2:-1]
        temp        = fields[14][2:-1]
        wet         = fields[15][1:] # These could be swapped <>
        warm        = fields[16][1:-1]
        hour        = date_for_merge.hour
        value = (hour,rainfall,temp,stationName,wet,warm,availableBikes,stationID,dayNumber,dayType)
        key         = int(hour)*100 + int(wet)*10000 +int(dayNumber) + int(stationID)*100000
        yield key, value

    def reducer(self, key, values):

        x = y = xsq = ysq = xy = 0.0
        n = totalBikesAvailable =  0


        for value in values:
            availableBikes  = int(value[6])
            hr              = int(value[0])
            wet             = int(value[4])
            stationID       = int(value[7])
            dayNumber       = int(value[8])
            dayType         = value[9]
            n += 1
            totalBikesAvailable += availableBikes
        averageBikes = totalBikesAvailable/n
        yield key, (key,hr,averageBikes,stationID,wet,dayNumber,dayType)


if __name__ == '__main__':
    ReduceLeftJoinEx.run()
