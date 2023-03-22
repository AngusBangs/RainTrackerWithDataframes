import unittest
import Conwy_Level_Predictor as CLP
import pandas as pd
from datetime import datetime

class TestData(unittest.TestCase):

    def test_data(self):
        Conwy,RainVariables,RainPowerVariables=CLP.ReadFiles()
        shape=Conwy.shape
        firstdate=pd.to_datetime(Conwy.index[0])
        lastdate=pd.to_datetime(Conwy.index[shape[0]-1])
        samplecount=(lastdate-firstdate).total_seconds()/900
        MultiEntry=Conwy.index.duplicated()
        #print('Duplicates')
        #print(Conwy[MultiEntry])

        #self.assertEqual(int(samplecount),shape[0])
        print(samplecount)
        print(shape[0])


        #print(Conwy.index)
        Conwy.reset_index(inplace=True)
        #print(Conwy.head())
        Conwy['Datetime']=Conwy['Datetime'].apply(lambda x: pd.to_datetime(x))
        TheroreticalDates=pd.DataFrame(pd.date_range(firstdate,lastdate,freq="15min"))
        TheroreticalDates.columns=['Datetime']

        #TheroreticalDates.reset_index(inplace=True)
        #print(TheroreticalDates.join(Conwy,how='outer'))

        #print(TheroreticalDates.head())
        #print(Conwy.head())
        #print(pd.concat([TheroreticalDates.set_index('Datetime'),Conwy.set_index('Datetime')], axis=1, join='outer'))
        combined=pd.concat([TheroreticalDates.set_index('Datetime'),Conwy.set_index('Datetime')], axis=1, join='outer')
        filt=combined['Precipitation'].isnull()
        print(combined[filt])
        #print(TheroreticalDates.join(Conwy,how='outer').head())
        #print(TheroreticalDates)
        #Conwy.join(TheroreticalDates,how='outer')
        #print(Conwy)
        #Conwy.to_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Test.csv')

if __name__ == '__main__':
    unittest.main()