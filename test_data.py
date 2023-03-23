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
        samplecount=(lastdate-firstdate).total_seconds()/900+1
        self.assertEqual(int(samplecount),shape[0])

#Confirm duplicate data is removed, data on 15 minute intervals only is used, and gaps are left in place
    def test_clean(self):
        TestDataFrame=pd.read_csv('C:\\Users\\angus\\OneDrive\\Documents\\Old Computer\\RainBot\\2023\\Test_Clean_Data.csv')
        Result=CLP.CleanData(TestDataFrame)
        self.assertEqual(Result.shape[0],8)

if __name__ == '__main__':
    unittest.main()