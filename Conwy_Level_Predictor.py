import pandas as pd
import random
import csv
import matplotlib.pyplot as plt
import urllib.request
import sys
import csv
import codecs

def GetForecast():
    try: 
        ResultBytes = urllib.request.urlopen("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/Ysbyty%2520Ifan?include=hours&key=YOUR_API_KEY&options=beta&contentType=csv")
        CSVText = csv.reader(codecs.iterdecode(ResultBytes, 'utf-8'))    
    except urllib.error.HTTPError  as e:
        ErrorInfo= e.read().decode() 
        print('Error code: ', e.code, ErrorInfo)
        sys.exit()
    except  urllib.error.URLError as e:
        ErrorInfo= e.read().decode() 
        print('Error code: ', e.code,ErrorInfo)
        sys.exit()
    return(CSVText)

def AddRiverLevelValues():
    ConwyLevels=pd.read_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Conwy_River_Levels.csv')
    ConwyLevels=CleanData(ConwyLevels)
    ConwyNewLevels=pd.read_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Additional_Conwy_River_Level_Data.csv')
    ConwyNewLevels.rename(columns={'Date (UTC)':'Datetime', 'Value':'Level'}, inplace=True)
    ConwyNewLevels=CleanData(ConwyNewLevels)
    ConwyLevels=pd.merge(ConwyLevels,ConwyNewLevels,how='outer',on=['Datetime','Level'])
    ConwyLevels.set_index('Datetime', inplace=True)
    ConwyLevels.sort_index().to_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Conwy_River_Levels.csv')

def AddRainLevelValues():
    ConwyRain=pd.read_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Conwy_Rain.csv')
    ConwyNewRain=pd.read_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Additional_Conwy_Rain.csv')
    ConwyRain=pd.merge(ConwyRain,ConwyNewRain,how='outer',on=['datetime','precip'])
    ConwyRain.drop_duplicates(subset='datetime',inplace=True)  
    ConwyRain.set_index('datetime', inplace=True)
    ConwyRain.sort_index().to_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Conwy_Rain.csv')
    ConwyRain.to_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Conwy_Rain.csv')

def CombineData():
    ConwyRain = pd.read_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Conwy_Rain.csv')
    ConwyLevels = pd.read_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Conwy_River_Levels.csv')
    ConwyRain.rename(columns={'datetime':'Datetime', 'precip':'Precipitation'}, inplace=True)

    ConwyRain['Datetime']=ConwyRain['Datetime'].str.replace('T',' ')
    ConwyLevels['Hour']=ConwyLevels['Datetime'].str[:13]+':00:00'
    
    ConwyRain.set_index('Datetime', inplace=True)
    ConwyLevels.set_index('Hour', inplace=True)
    Conwy=ConwyLevels.join(ConwyRain)
    Conwy=Conwy[Conwy['Precipitation'].notnull()]
    Conwy.set_index('Datetime', inplace=True)
    (Conwy).sort_index(inplace=True)
    (Conwy).to_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\CombinedData.csv')
    return(Conwy)

def ItterateVariables(RainVariables, RainPowerVariables):
    pastrain=[0]*288
    RainComponents=[0]*len(pastrain)
    RainComponentsItteration=RainComponents.copy()
    RainVariablesItteration=RainVariables.copy()
    RainPowerVariablesItteration=RainPowerVariables.copy()
    for j in range(0,len(RainVariables)):
        RainVariablesItteration[j]=RainVariables[j]*(0.98+random.random()*0.04)
    for j in range(0,len(RainPowerVariables)):
        RainPowerVariablesItteration[j]=RainPowerVariables[j]*(0.98+random.random()*0.04)
    for i in range(1,len(Conwy['Prediction'])):
        for j in range(0,len(pastrain)-1):
            pastrain[j]=pastrain[j+1]
            RainComponents[j]=RainVariables[j]*(pastrain[j]**RainPowerVariables[j]) 
            RainComponentsItteration[j]=RainVariablesItteration[j]*(pastrain[j]**RainPowerVariablesItteration[j]) 
        pastrain[int(len(pastrain)-1)]=Conwy['Precipitation'][i]
        Conwy['Prediction'][i]=(Conwy['Prediction'][i-1]**RainPowerVariables[-1])*RainVariables[-2]+RainVariables[-1]+sum(RainComponents)
        Conwy['Prediction_Itteration'][i]=(Conwy['Prediction_Itteration'][i-1]**RainPowerVariablesItteration[-1])*RainVariablesItteration[-2]+RainVariablesItteration[-1]+sum(RainComponentsItteration)

    filt=Conwy['Level'].notnull()

    if abs((Conwy['Prediction'][filt]-Conwy['Level'][filt])).sum() > abs((Conwy['Prediction_Itteration']-Conwy['Level'])).sum():
        RainPowerVariables=RainPowerVariablesItteration.copy()
        RainVariables=RainVariablesItteration.copy()
        print(abs((Conwy['Prediction']-Conwy['Level'])).sum())
        with open('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Variables.csv', 'w') as f:
            write = csv.writer(f)
            write.writerow(RainVariables)
            f.close()
        with open('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\PowerVariables.csv', 'w') as f:
            write = csv.writer(f)
            write.writerow(RainPowerVariables)
            f.close()
    return(RainVariables,RainPowerVariables)

def ReadFiles():
    Conwy=pd.read_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\CombinedData.csv')
    Conwy.set_index('Datetime', inplace=True)
    Conwy.sort_index(inplace=True)

    file = open("C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Variables.csv", "r")
    RainVariables = list(csv.reader(file, delimiter=","))[0].copy()
    file.close()

    file = open("C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\PowerVariables.csv", "r")
    RainPowerVariables = list(csv.reader(file, delimiter=","))[0].copy()
    file.close()

    for j in range(0,len(RainVariables)):
        RainVariables[j]=float(RainVariables[j])
        
    for j in range(0,len(RainPowerVariables)):
        RainPowerVariables[j]=float(RainPowerVariables[j])
    return(Conwy,RainVariables,RainPowerVariables)

def ReadForecast():
    Forecast=pd.read_csv("C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Forecast_Rain_Cowny.csv")
    Forecast.rename(columns={'datetime':'Datetime','precip':'Precipitation'}, inplace=True)
    Forecast.drop('precipprob', axis=1, inplace=True)
    Forecast['Datetime']=Forecast['Datetime'].str.replace('T',' ')

    firstdate=pd.to_datetime(Forecast['Datetime'][0])
    lastdate=pd.to_datetime(Forecast['Datetime'][Forecast.shape[0]-1])
    ForecastLevels=pd.DataFrame(pd.date_range(firstdate,lastdate,freq="15min"))
    ForecastLevels.columns=['Datetime']
    
    ForecastLevels['Datetime']=ForecastLevels['Datetime'].apply(lambda x: str(x))
    ForecastLevels['Hour']=ForecastLevels['Datetime'].str[:13]+':00:00'
    ForecastLevels.set_index('Hour',inplace=True)
    Forecast.set_index('Datetime',inplace=True)

    Forecast=ForecastLevels.join(Forecast)
    Forecast.set_index('Datetime',inplace=True)
    #print(Forecast)
    return(Forecast)

def CleanData(Dataframe):
    Dataframe.drop_duplicates(subset='Datetime',inplace=True)    
    firstdate=pd.to_datetime(Dataframe['Datetime'][0])
    Dataframe.reset_index(inplace=True)
    Dataframe.drop(['index'],axis=1,inplace=True)
    lastdate=pd.to_datetime(Dataframe['Datetime'][Dataframe.shape[0]-1])
    Dataframe['Datetime']=Dataframe['Datetime'].apply(lambda x: pd.to_datetime(x))
    TheoreticalDates=pd.DataFrame(pd.date_range(firstdate,lastdate,freq="15min"))
    TheoreticalDates.columns=['Datetime']
    Dataframe=pd.concat([TheoreticalDates.set_index('Datetime'),Dataframe.set_index('Datetime')],axis=1,join='outer')

    Dataframe.reset_index(inplace=True)
    Dataframe['SecondsTime']=pd.to_datetime(Dataframe['Datetime']).apply(lambda x: (x-firstdate).total_seconds()%900)
    Dataframe.drop(Dataframe[Dataframe['SecondsTime']!=0].index, inplace=True)
    Dataframe.drop(['SecondsTime'],axis=1,inplace=True)
    return(Dataframe)

def ForecastLevel(Forecast):

    Forecast=pd.concat([(Conwy.query('Datetime>"2023-03-25 11:00:00"')),(Forecast.query('Datetime>"2023-03-28 12:00:00"'))])
    print(Forecast['Level'][287])
    pastrain=Forecast['Precipitation'][:288]
    RainComponents=[0]*len(pastrain)
    Forecast['Prediction']=Forecast['Level']
    for i in range(288,1004):
        for j in range(0,len(pastrain)-1):
            pastrain[j]=pastrain[j+1]
            RainComponents[j]=RainVariables[j]*(pastrain[j]**RainPowerVariables[j]) 
        pastrain[int(len(pastrain)-1)]=Forecast['Precipitation'][i]
        Forecast['Prediction'][i]=(Forecast['Prediction'][i-1]**RainPowerVariables[-1])*RainVariables[-2]+RainVariables[-1]+sum(RainComponents)

    #fig, ax=plt.subplots()

    print(Forecast.index[191])
    plt.plot(Forecast.index[191:672],Forecast['Level'][191:672], color='k', label='River Level (m)')
    plt.plot(Forecast.index[191:288],Forecast['Precipitation'][191:288], color='b', label='Rainfall (mm/hr)')
    plt.plot(Forecast.index[287:672],Forecast['Precipitation'][287:672], color='b', linestyle='--')
    plt.plot(Forecast.index[287:672],Forecast['Prediction'][287:672], color='k', linestyle='--')
    plt.xticks(['2023-03-27 12:00:00','2023-03-27 18:00:00','2023-03-28 00:00:00','2023-03-28 06:00:00','2023-03-28 12:00:00','2023-03-28 18:00:00','2023-03-29 00:00:00','2023-03-29 06:00:00','2023-03-29 12:00:00','2023-03-29 18:00:00','2023-03-30 00:00:00','2023-03-30 06:00:00','2023-03-30 12:00:00','2023-03-30 18:00:00','2023-03-31 00:00:00','2023-03-31 06:00:00','2023-03-31 12:00:00','2023-03-31 18:00:00','2023-04-01 00:00:00','2023-04-01 06:00:00','2023-04-01 12:00:00'])
    plt.grid(True)
    fig = plt.gcf()
    fig.autofmt_xdate(rotation=45)
    
    plt.title('River Conwy (Cwmlanerch gauge)')
    plt.plot(['2023-03-27 12:00:00','2023-04-01 12:00:00'],[2,2], color='g', label='High')
    plt.plot(['2023-03-27 12:00:00','2023-04-01 12:00:00'],[1.5,1.5], color='y', label='Medium')
    plt.plot(['2023-03-27 12:00:00','2023-04-01 12:00:00'],[1,1], color='r', label='Low')

    plt.legend()
    plt.show()


def BestFit():
    Conwy['Prediction']=Conwy['Level'].copy()
    Conwy['Prediction_Itteration']=Conwy['Prediction'].copy()
    Conwy['Prediction'][0]=Conwy['Level'][0]
    Conwy['Prediction_Itteration'][0]=Conwy['Level'][0]

    for cycle in range(1,2):
       RainVariables,RainPowerVariables=ItterateVariables(RainVariables, RainPowerVariables)
       print(cycle)

    plt.plot(Conwy.index,Conwy['Level'], color='k', label='Level (m)')
    plt.plot(Conwy.index,Conwy['Precipitation'], color='b', label='Rainfall (mm/hr)')
    plt.plot(Conwy.index,Conwy['Prediction'], color='k', linestyle='--', label='Predicted Level (m)')
    
    plt.title('River Conwy Rain')
    plt.legend()

    plt.plot([min(Conwy.index),max(Conwy.index)],[1,1])
    plt.plot([min(Conwy.index),max(Conwy.index)],[1.5,1.5])
    plt.plot([min(Conwy.index),max(Conwy.index)],[2,2])
    plt.show()

if __name__ == '__main__':
    AddRiverLevelValues()
    AddRainLevelValues()
    Conwy=CombineData()
    Conwy,RainVariables,RainPowerVariables=ReadFiles()
    Forecast=ReadForecast()
    ForecastLevel(Forecast)