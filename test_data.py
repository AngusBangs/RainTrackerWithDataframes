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


if __name__ == '__main__':
    unittest.main()