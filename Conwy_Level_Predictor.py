import pandas as pd
import random
import csv
import datetime
import matplotlib.pyplot as plt

def AddRiverLevelValues():
    ConwyLevels=pd.read_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Conwy_River_Levels.csv')
    CleanData(ConwyLevels)
    ConwyNewLevels=pd.read_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Additional_Conwy_River_Level_Data.csv')
    ConwyNewLevels.rename(columns={'Date (UTC)':'Datetime', 'Value':'Level'}, inplace=True)
    CleanData(ConwyNewLevels)
    ConwyLevels=pd.merge(ConwyLevels,ConwyNewLevels,how='outer',on=['Datetime','Level'])
    ConwyLevels.set_index('Datetime', inplace=True)
    ConwyLevels.sort_index().to_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Conwy_River_Levels.csv')

def AddRainLevelValues():
    ConwyRain=pd.read_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Conwy_Rain.csv')
    ConwyNewRain=pd.read_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Additional_Conwy_Rain.csv')
    ConwyRain=pd.merge(ConwyRain,ConwyNewRain,how='outer',on=['datetime','precip'])
    ConwyRain.set_index('datetime', inplace=True)
    ConwyRain.sort_index().to_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Conwy_Rain.csv')

def CombineData():
    ConwyRain = pd.read_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Conwy_Rain.csv')
    ConwyLevels = pd.read_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Conwy_River_Levels.csv')
    ConwyRain.rename(columns={'datetime':'Datetime', 'precip':'Precipitation'}, inplace=True)

    ConwyRain['Datetime']=ConwyRain['Datetime'].str.replace('T',' ')
    ConwyLevels['Hour']=ConwyLevels['Datetime'].str[:13]+':00:00'
    
    ConwyRain.set_index('Datetime', inplace=True)
    ConwyLevels.set_index('Hour', inplace=True)
    Conwy=ConwyLevels.join(ConwyRain).dropna()
    Conwy.set_index('Datetime', inplace=True)
    (Conwy).sort_index(inplace=True)
    (Conwy).to_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\CombinedData.csv')
    return()

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

    if abs((Conwy['Prediction']-Conwy['Level'])).sum() > abs((Conwy['Prediction_Itteration']-Conwy['Level'])).sum():
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
    Conwy.drop_duplicates()

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

def CleanData(Dataframe):
    Dataframe.sort_values(by=['Datetime'],inplace=True)
    firstdate=pd.to_datetime(Dataframe['Datetime'][0])

    lastdate=pd.to_datetime(Dataframe['Datetime'][Dataframe.shape[0]-1])

    StepFrame=Dataframe.copy()
    StepFrame.reset_index(inplace=True)
    StepFrame['Datetime']=StepFrame['Datetime'].apply(lambda x: pd.to_datetime(x))
    StepFrame=StepFrame.drop('index',axis=1)
    TheoreticalDates=pd.DataFrame(pd.date_range(firstdate,lastdate,freq="15min"))
    TheoreticalDates.columns=['Datetime']
    #print(StepFrame)
    #print(TheoreticalDates)
    combined=pd.concat([TheoreticalDates.set_index('Datetime'),StepFrame.set_index('Datetime')],axis=1,join='outer')
    filt=combined['Level'].isnull()
    print(combined[filt])

    Dataframe['SecondsTime']=pd.to_datetime(Dataframe['Datetime']).apply(lambda x: (x-firstdate).total_seconds()%900)
    Dataframe.drop(Dataframe[Dataframe['SecondsTime']!=0].index, inplace=True)
    Dataframe.drop(['SecondsTime'],axis=1,inplace=True)

if __name__ == '__main__':
    AddRiverLevelValues()
    AddRainLevelValues()
    CombineData()
    Conwy,RainVariables,RainPowerVariables=ReadFiles()

    # Conwy['Prediction']=Conwy['Level'].copy()
    # Conwy['Prediction_Itteration']=Conwy['Prediction'].copy()
    # Conwy['Prediction'][0]=Conwy['Level'][0]
    # Conwy['Prediction_Itteration'][0]=Conwy['Level'][0]

#     for cycle in range(1,10000):
#         RainVariables,RainPowerVariables=ItterateVariables(RainVariables, RainPowerVariables)
#         print(cycle)

#     plt.plot(Conwy.index,Conwy['Level'])
#     plt.plot(Conwy.index,Conwy['Precipitation'])
#     plt.plot(Conwy.index,Conwy['Prediction'])

#     plt.plot([min(Conwy.index),max(Conwy.index)],[1,1])
#     plt.plot([min(Conwy.index),max(Conwy.index)],[1.5,1.5])
#     plt.plot([min(Conwy.index),max(Conwy.index)],[2,2])
#     plt.show()