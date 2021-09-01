#! /usr/bin/python3
# ReduceLeftJoin.py
# Joining the bikes and the Weather datasets
import datetime as dt
import pandas as pd
from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

COVID_HEADER = ('X,Y,Date,ConfirmedCovidCases,TotalConfirmedCovidCases,ConfirmedCovidDeaths,TotalCovidDeaths,StatisticsProfileDate,CovidCasesConfirmed,HospitalisedCovidCases,RequiringICUCovidCases,HealthcareWorkersCovidCases,ClustersNotified,HospitalisedAged5,HospitalisedAged5to14,HospitalisedAged15to24,HospitalisedAged25to34,HospitalisedAged35to44,HospitalisedAged45to54,HospitalisedAged55to64,Male,Female,Unknown,Aged1to4,Aged5to14,Aged15to24,Aged25to34,Aged35to44,Aged45to54,Aged55to64,Median_Age,CommunityTransmission,CloseContact,TravelAbroad,FID,HospitalisedAged65to74,HospitalisedAged75to84,HospitalisedAged85up,Aged65to74,Aged75to84,Aged85up')

class ReduceLeftJoinEx(MRJob):

    OUTPUT_PROTOCOL = JSONValueProtocol 

    def mapper(self, _, line):
        line=line.strip()
        if line != COVID_HEADER:
            if fields[0][0]=="-": # Covid Dataset             
                date_for_merge = pd.to_datetime(dt.datetime.strptime(fields[2][:19],"%Y/%m/%d %H:%M:%S")).floor('d') #Had to change these due to space removal
                confirmedCases = fields[3]
                totalCases = fields[4]
                confirmedDeaths = fields[5]
                totalDeaths = fields[6]
                hospitilized = fields[7]
                value = (confirmedCases,totalCases,confirmedDeaths,totalDeaths,hospitilized)
                key = date_for_merge.strftime("%Y/%m/%d")
                mapped.append((key, ('CO', value)))        


            else:
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
                yield key, ('BI', value)
                    #else:
                        #raise ValueError('An input file does not contain the required number of fields.')

    def reducer(self, key, values):
        total = 0
        count = 0
        finishedStations =[]
        weather_tuples = []
        bike_tuples = []
        for value in list(values):
            relation = value[0]
            #print(type(relation))    # either 'BI' or 'CO'
            if relation == 'CO':  # Covid Data
                confirmedCases  =   value[0]
                totalCases      =
                confirmedDeaths =
                totalDeaths     = 
                hospitilized    =

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
                if stationID not in finishedStations:
                    bike_tuples.append((date_for_merge,stationID,name,bikeStands,availableBikes,address,lattitude,longitude,occupancy,full,empty,dayNumber,dayType))
                    finishedStations.append(stationID)
                #print("getting through the bikes")
                #yield(key,(stationID,lastUpdate))

            elif relation == 'BI':  # Weather Data
                date = value[0]
                name = value[1]
                wet  = int(value[2])
                totalBikeUse  = float(value[3])
                rain = float(value[4])
                stationID = int(value[6])
                weather_tuples.append((rain,temp,wet,warm))
                #yield(key,(name,rain))
            else:
                raise ValueError('An unexpected join key was encountered.',+relation)
            #if len(bike_tuples) > 0:
            #if (rain in locals()) and (stationID in locals()):
            if (len(covid_tuples) > 0 and len(covid_tuples)>0):
                for weather_tuple in weather_tuples:
                    #for bike_tuple in bike_tuples:
                    #keyList = (),key
                    yield key,(bike_tuples[-1] + weather_tuple)
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
