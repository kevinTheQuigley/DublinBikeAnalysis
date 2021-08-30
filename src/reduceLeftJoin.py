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
                    date_for_merge = pd.to_datetime(dt.datetime.strptime(fields[0], "%d-%b-%Y %H:%M")).floor('H')
                    if date_for_merge > dt.datetime.strptime('2020-01-01',"%Y-%m-%d"):
                        #date_for_merge = date.dt.round('H')
                        rain = fields[2]
                        temp = fields[4] 
                        dry = float(rain) > 0
                        warm = float(temp) > 18
                        value = (rain,temp,dry,warm)
                        key = date_for_merge.strftime("%Y/%m/%d %H:%M")
                        yield key, ('WE', value)
                else: #fields[0][2] =="-": # Bike Dataset
                    stationID =     fields[0]
                    lastUpdated =   fields[2]
                    name =          fields[3]
                    bikeStands =    fields[4]
                    availableBikes= fields[5]
                    address =       fields[7]
                    lattitude =     fields[8]
                    longitude =     fields[9]

                    #Creating occupancy statistics
                    occupancy   = int(availableBikes) / int(bikeStands)
                    full        = int(occupancy == 0)
                    empty       = int(occupancy == 1)
                    date_for_merge = pd.to_datetime(dt.datetime.strptime(fields[1],  "%Y-%m-%d %H:%M:%S")).floor('H')

                    dayNumber   = date_for_merge.dayofweek 
                    if dayNumber <= 4:
                        dayType = 'Weekday'
                    elif dayNumber == 5:
                        dayType = 'Saturday'
                    else:
                        dayType = 'Sunday'
                    #Returning Key and Value
                    key = date_for_merge.strftime("%Y/%m/%d %H:%M")
                    value = (stationID,name,bikeStands,availableBikes,address,lattitude,longitude,occupancy,full,empty,dayNumber,dayType,key)
                    yield key, ('BI', value)
                #else:
                    #raise ValueError('An input file does not contain the required number of fields.')

    def reducer(self, key, values):
        total = 0
        count = 0

        weather_tuples = []
        bike_tuples = []
        for value in list(values):
            relation = value[0]
            #print(type(relation))    # either 'BI' or 'WE'
            if relation == 'BI':  # orders data
                date_for_merge  =   value[1][12]
                stationID       =   value[1][0]
                name            =   value[1][1]
                #lastUpdate      =  value[1][1]
                bikeStands      =   value[1][2]
                availableBikes  =   value[1][3]
                address         =   value[1][4]
                lattitude       =   value[1][5]
                longitude       =   value[1][6]
                occupancy       =   value[1][7]
                full            =   value[1][8]
                empty           =   value[1][9]
                dayNumber       =   value[1][10]
                dayType         =   value[1][11]
                bike_tuples.append((date_for_merge,stationID,name,bikeStands,availableBikes,address,lattitude,longitude,occupancy,full,empty,dayNumber,dayType))
                #print("getting through the bikes")
                #yield(key,(stationID,lastUpdate))

            elif relation == 'WE':  # Weather Data
                rain            = value[1][0]
                temp            = value[1][1]
                dry             = value[1][2]
                warm            = value[1][2]
                weather_tuples.append((key, rain,temp,dry,warm))
                #yield(key,(name,rain))
            else:
                raise ValueError('An unexpected join key was encountered.',+relation)
            #if len(bike_tuples) > 0:
            #if (rain in locals()) and (stationID in locals()):
            if len(weather_tuples) > 0 and len(bike_tuples)>0:
                for weather_tuple in weather_tuples:
                    for bike_tuple in bike_tuples:
                        yield key, (bike_tuple,weather_tuple)
                bike_tuples = []
            
            '''if len(bike_tuples) > 0:
                for bike_values in bike_tuples:
                    if len(weather_tuples) > 0:
                        count = len(weather_tuples)
                        if count != 0:  
                            total = sum(int(weather_tuples[0][1]))
                        else:
                            total = 0
                        output = [key] + [o for o in bike_values] + [count, total]
                    else:
                        output = [key] + [o for o in bike_values] + ['null','null']
                    yield None, output
        #else:
        #    raise ValueError('An unexpected join type was encountered. This should not happen.')'''

if __name__ == '__main__':
    ReduceLeftJoinEx.run()
