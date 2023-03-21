import pandas as pd
import random
import csv

def AddRiverLevelValues():
    ConwyLevels=pd.read_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Conwy_River_Levels.csv')
    ConwyNewLevels=pd.read_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Additional_Conwy_River_Level_Data.csv')
    ConwyNewLevels.rename(columns={'Date (UTC)':'Datetime', 'Value':'Level'}, inplace=True)
    ConwyLevels=pd.merge(ConwyLevels,ConwyNewLevels,how='outer',on=['Datetime','Level'])
    ConwyLevels.set_index('Datetime', inplace=True)
    ConwyLevels.sort_index().to_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Conwy_River_Levels.csv')

    
def AddRainLevelValues():
    ConwyRain=pd.read_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Conwy_Rain.csv')
    ConwyNewRain=pd.read_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Additional_Conwy_Rain.csv')
    ConwyRain=pd.merge(ConwyRain,ConwyNewRain,how='outer',on=['datetime','precip'])
    ConwyRain.set_index('datetime', inplace=True)
    ConwyRain.sort_index().to_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Conwy_Rain.csv')

def importValues():
    ConwyRain = pd.read_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Conwy_Rain.csv')
    ConwyLevels = pd.read_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Conwy_River_Levels.csv')
    #schema_df = pd.read_csv('')
    ConwyRain.rename(columns={'datetime':'Datetime', 'precip':'Precipitation'}, inplace=True)

    ConwyRain['Datetime']=ConwyRain['Datetime'].str.replace('T',' ')
    ConwyLevels['Hour']=ConwyLevels['Datetime'].str[:13]+':00:00'

    ConwyRain.set_index('Datetime', inplace=True)
    ConwyLevels.set_index('Hour', inplace=True)
    #ConwyLevels.set_index('Datetime', inplace=True)

    print('Conwy rain')
    print(ConwyRain)
    print(ConwyRain.loc['2023-02-01 00:00:00'])
    print('Conwy Levels')
    print(ConwyLevels)
    print(ConwyLevels.loc['2023-02-01 00:00:00'])

    #ConwyLevels.reset_index(inplace=True)

    #print(ConwyRain.head())
    #print(ConwyLevels.head())

    #print(ConwyRain)
    #print(ConwyLevels)
    #ConwyRain.join(ConwyLevels)#.dropna()
    print('print')
    print(ConwyLevels.join(ConwyRain))
    print('ConwyA')
    ConwyA=ConwyLevels.join(ConwyRain).dropna()
    #print(ConwyLevels)
    #print(ConwyA)
    #print((ConwyA))
    #Conwy.set_index('Datetime', inplace=True)
    (ConwyA).sort_index(inplace=True)
    (ConwyA).to_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\CombinedData.csv')
    return(ConwyA)

AddRiverLevelValues()
AddRainLevelValues()
Conwy=importValues()

Conwy=pd.read_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\CombinedData.csv')
Conwy.set_index('Datetime', inplace=True)
Conwy.sort_index(inplace=True)

def ItterateVariables(RainVariables, RainPowerVariables):
    pastrain=[0]*288
    RainComponents=[0]*len(pastrain)

    RainComponentsItteration=RainComponents.copy()
    RainVariablesItteration=RainVariables.copy()
    RainPowerVariablesItteration=RainPowerVariables.copy()
    for j in range(0,len(RainVariables)):
        RainVariables[j]=float(RainVariables[j])
        RainVariablesItteration[j]=RainVariables[j]*(0.98+random.random()*0.04)
    for j in range(0,len(RainPowerVariables)):
        RainPowerVariables[j]=float(RainPowerVariables[j])
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
        #print(RainComponents)
        RainPowerVariables=RainPowerVariablesItteration.copy()
        #print(RainComponents)
        RainVariables=RainVariablesItteration.copy()
        print(abs((Conwy['Prediction']-Conwy['Level'])).sum())
        with open('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Variables.csv', 'w') as f:
            # using csv.writer method from CSV package
            write = csv.writer(f)
            write.writerow(RainVariables)
            f.close()
        with open('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\PowerVariables.csv', 'w') as f:
            # using csv.writer method from CSV package
            write = csv.writer(f)
            write.writerow(RainPowerVariables)
            f.close()
    return(RainVariables,RainPowerVariables)

# Conwy['Prediction']=Conwy['Level'].copy()
# Conwy['Prediction_Itteration']=Conwy['Prediction'].copy()
# Conwy['Prediction'][0]=Conwy['Level'][0]
# Conwy['Prediction_Itteration'][0]=Conwy['Level'][0]

# file = open("C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Variables.csv", "r")
# RainVariables = list(csv.reader(file, delimiter=","))[0].copy()
# file.close()

# file = open("C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\PowerVariables.csv", "r")
# RainPowerVariables = list(csv.reader(file, delimiter=","))[0].copy()
# file.close()

#for cycle in range(1,100000):
#    RainVariables,RainPowerVariables=ItterateVariables(RainVariables, RainPowerVariables)
#    print(cycle)

# plt.plot(Conwy.index,Conwy['Level'])
# plt.plot(Conwy.index,Conwy['Precipitation'])
# plt.plot(Conwy.index,Conwy['Prediction'])

# plt.plot([min(Conwy.index),max(Conwy.index)],[1,1])
# plt.plot([min(Conwy.index),max(Conwy.index)],[1.5,1.5])
# plt.plot([min(Conwy.index),max(Conwy.index)],[2,2])
# plt.show()
