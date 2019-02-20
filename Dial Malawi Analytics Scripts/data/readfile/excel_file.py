
import pandas as pd

def read_data(filename):
    df_file = pd.read_excel(filename)
    return df_file

def readcsv(filename):
    df_file = pd.read_csv(filename)
    return df_file

def write_data(filename,df_write):
    df_write.to_excel(filename)
    return

