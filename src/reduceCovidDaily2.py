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
        fields = line.split(',')
        if line != COVID_HEADER and len(line) > 20:
            if fields[0][0]=="-": # Covid Dataset             
                date_for_merge = pd.to_datetime(dt.datetime.strptime(fields[2][:19],"%Y/%m/%d %H:%M:%S")).floor('d')
                confirmedCases = fields[3]
                totalCases = fields[4]
                confirmedDeaths = fields[5]
                totalDeaths = fields[6]
                hospitilized = fields[9]
                value = (confirmedCases,totalCases,confirmedDeaths,totalDeaths,hospitilized)
                key = date_for_merge.strftime("%Y/%m/%d")
                yield key, ('CO', value)


            elif fields[0][0]=="[":
                date_for_merge = pd.to_datetime(dt.datetime.strptime(fields[0][2:-1],"%Y/%m/%d"))
                date_for_value = date_for_merge.strftime("%Y/%m/%d")
                stationName = fields[1][2:-1]
                rainyDay    = fields[2][2:-1] # These could be swapped <>
                totalBikeUse= fields[3]
                rainfall    = fields[4]
                avgTemp     = fields[5]
                stationID   = fields[7]
                value = (date_for_value,stationName,rainyDay,totalBikeUse,rainfall,avgTemp,stationID)
                key = date_for_merge.strftime("%Y/%m/%d") 
                yield key, ('BI', value) 
            else: 
                g =0
                g +=1
                #raise ValueError('An input file does not contain the required number of fields.')

    def reducer(self, key, values):
        total = 0
        count = 0
        covid_tuples = []
        bike_tuples = []
        totalBikeUse = 0
        for value in list(values):
            relation = value[0]
            #print(type(relation))    # either 'BI' or 'CO'
            if relation == 'CO':  # Covid Data
                confirmedCases  =   int(value[1][0])
                totalCases      =   int(value[1][1])
                if value[1][2] != "":
                    confirmedDeaths =   int(value[1][2])
                else:
                    confirmedDeaths = 0
                totalDeaths     =   int(value[1][3])
                if value[1][4] != "":
                    hospitilized    =   int(value[1][4])
                else:
                    hospitilized = 0
                covid_tuples.append((confirmedCases,totalCases,confirmedDeaths,hospitilized))
                #print("getting through the bikes")
                #yield(key,(stationID,lastUpdate))

            elif relation == 'BI':  # Weather Data
                date = value[1][0]
                name = value[1][1]
                wet  = int(value[1][2])
                totalBikeUse  = float(value[1][3])
                rain = float(value[1][4])
                stationID = int(value[1][6])
                bike_tuples.append((date,stationID,name,totalBikeUse,wet,rain))
                #yield(key,(name,rain))
            else:
                raise ValueError('An unexpected join key was encountered.',+relation)
            #if len(bike_tuples) > 0:
            #if (rain in locals()) and (stationID in locals()):
            if (len(bike_tuples) > 0 and len(covid_tuples)>0):
                for bike_tuple in bike_tuples:
                    #for bike_tuple in bike_tuples:
                    #keyList = (),key
                    totalBikeUse += bike_tuple[3]
                date   = bike_tuples[-1][0]
                wet   = bike_tuples[-1][4]
                rain  = bike_tuples[-1][5]
                finalBikeTuple = (date,totalBikeUse,wet,rain)
                bike_tuples = []
                total +=1
        if total != 0:
            yield key,(finalBikeTuple + covid_tuples[-1])
            
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
