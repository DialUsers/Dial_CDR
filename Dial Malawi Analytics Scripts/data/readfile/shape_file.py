
###########################
#read and write shapefiles
###########################

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import geopandas as gpd

def read_shape_data_file(filename):
    data=gpd.read_file(filename)
    return data
    
def write_shape_data_file(filename, df_write):
    df_write.to_file(filename)
    return

