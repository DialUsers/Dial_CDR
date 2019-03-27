# -*- coding: utf-8 -*-
"""
Created on Mon Mar 18 11:05:54 2019
This module is used for reading and writing the csv files.
"""

import pandas as pd

def write_csv_data_file(dir_csv, filename, df_write):
    location = dir_csv + filename + '.csv'
    df_write.to_csv(location, index=False )
    return
    
def read_csv_data_file(dir_csv, filename):
    location = dir_csv + filename + '.csv'
    df = pd.read_csv(location)
    return df
    