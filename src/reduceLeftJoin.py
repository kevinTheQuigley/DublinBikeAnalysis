#! /usr/bin/python3
# ReduceLeftJoin.py
# Joining the bikes and the Weather datasets
import datetime as dt
import pandas as pd
from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

#WEATHER_HEADER=('date_for_merge|rain|temp|wetb|dewpt|vappr|rhum|msl|dry|warm')
#BIKE_HEADER = ('date_for_merge|STATION ID|LAST UPDATED|NAME|BIKE STANDS|AVAILABLE BIKE STANDS|AVAILABLE BIKES|STATUS|ADDRESS|LATITUDE|LONGITUDE|DATETIME|DATE|OCCUPANCY_PCT|FULL|EMPTY|DAY_NUMBER|DAY_TYPE|TIME_TYPE|HOUR|MONTH|CLUSTER_GROUP')
WEATHER_HEADER=('date,ind,rain,ind,temp,ind,wetb,dewpt,vappr,rhum,msl')
BIKE_HEADER = ('STATION ID,TIME,LAST UPDATED,NAME,BIKE STANDS,AVAILABLE BIKE STANDS,AVAILABLE BIKES,STATUS,ADDRESS,LATITUDE,LONGITUDE')

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
        if line != BIKE_HEADER and line != WEATHER_HEADER:
            if len(line)>=len(WEATHER_HEADER):
                fields = line.split(',')
                if fields[10][0]!="-": # Weather Dataset
                    date_for_merge = pd.to_datetime(dt.datetime.strptime(fields[0],"%d-%b-%Y %H:%M")).floor('H')
                    #date_for_merge = date.dt.round('H')
                    rain = fields[1]
                    temp = fields[2] 
                    value = (rain,temp)
                    key = date_for_merge.strftime("%Y/%m/%d %H:%M")
                    yield key, ('BI', value)
                else: #fields[0][2] =="-": # Bike Dataset
                    stationID = fields[0]
                    name = fields[3] 
                    date_for_merge = pd.to_datetime(dt.datetime.strptime(fields[1],  "%Y-%m-%d %H:%M:%S")).floor('H')
                    #date_for_merge = dateTime.dt.round('H')
                    value = (stationID,name)
                    key = date_for_merge.strftime("%Y/%m/%d %H:%M")
                    yield key, ('WE', value)
                #else:
                    #raise ValueError('An input file does not contain the required number of fields.')

    def reducer(self, key, values):
        total = 0
        count = 0
        weather_tuples = []
        bike_tuples = []

        for value in list(values):
            relation = value[0]  # either 'BI' or 'WE'
            if relation == 'BI':  # orders data
                rain = value[1][0]
                temp = value[1][1]
                weather_tuples.append((key, rain,temp))
            elif relation == 'WE':  # Weather Data
                stationID = value[1][0]
                lastUpdate = value[1][1]
                name = value[1][2]
                bike_tuples.append(stationID,lastUpdate,name)
            else:
                raise ValueError('An unexpected join key was encountered.',+relation)
            if len(bike_tuples) > 0:
                for bike_values in order_tuples:
                    if len(weather_tuples) > 0:
                        count = len(detail_tuples)
                        total = sum(detail_tuples)
                        output = [key] + [o for o in bike_values] + [count, total]
                    else:
                        output = [key] + [o for o in bike_values] + ['null','null']
                    yield None, output
        else:
            raise ValueError('An unexpected join type was encountered. This should not happen.')

if __name__ == '__main__':
    ReduceLeftJoinEx.run()
