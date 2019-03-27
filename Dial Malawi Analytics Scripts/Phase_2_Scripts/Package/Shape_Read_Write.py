# -*- coding: utf-8 -*-
"""
Created on Thu Mar 14 16:04:48 2019
This Module is used for reading and Writing the Shape Files
"""

import geopandas as gpd

def read_shape_data_file(dir_shp_read, filename):
    location = dir_shp_read + filename + '.shp'
    data=gpd.read_file(location)
    return data

def write_shape_data_file(dir_shp_write, filename, df_write):
    location = dir_shp_write + filename + '.shp'
    df_write.to_file(location)
    return
