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
    def configure_args(self):
        super(ReduceLeftJoinEx, self).configure_args()
        self.add_passthru_arg(
            '--join_type',
            default = 'left_outer',
            choices=['inner', 'left_outer','right_outer'],
            help="The type of join"
        )

    def mapper(self, _, line):
        line=line.strip()
        fields = line.split(',')
        date_for_merge = pd.to_datetime(dt.datetime.strptime(fields[0][2:16],"%Y/%m/%d %H:")).floor('h') #Had to change these due to space removal
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
        value = (hour,rainfall,temp,stationName,wet,warm,availableBikes,date_for_merge)
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
        for value in list(values):
            hour = value[0]
            rain = value[1]
            temp = value[2]
            name = value[3]
            wet  = value[4]
            warm = value[5]
            key  = value[7]
            availableBikes  = int(value[6])
            if oldBikeAvailability  != -1:
                bikeDelta = oldBikeAvailability - availableBikes
            else:
                bikeDelta = 0
            totalBikeChange += bikeDelta
            totalWet += int(wet)
        value = (name,totalWet,BikeDelta)
        yield(key, value)


if __name__ == '__main__':
    ReduceLeftJoinEx.run()
