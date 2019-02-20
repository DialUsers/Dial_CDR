# -*- coding: utf-8 -*-

import pandas as pd 
import numpy as np 
import datetime 
import csv 
import geopandas as gpd
from datetime import datetime 
import pyodbc 
import os 
from collections import defaultdict, Counter 
from numpy.random import randn
from functools import reduce
import itertools  
from joblib import Parallel, delayed
import multiprocessing
from shapely.geometry import Point, LineString, Polygon, shape, multipolygon
from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="specify_your_app_name_here")

#read data from HIVE
def readdatahive(connection,table_name):
    query = "select * from etldb.{}".format(table_name) 
        
    input_data = pd.read_sql_query(query, connection) 
        
    return input_data

#Read shapefile
def read_shape_data_file(dir_shp_read, filename):
    location = dir_shp_read + filename + '.shp'
    data=gpd.read_file(location)
    return data


#Groupby to get number of call_originating_numbers on each date and for each time window
def time_groupby(data):
    time_7to9=data.groupby(['call_start_date','7amto9am'])['call_originating_number'].count().reset_index(name="7to9_count").rename(columns={'7amto9am':'cell_id'})
    time_9to11=data.groupby(['call_start_date','9amto11am'])['call_originating_number'].count().reset_index(name="9to11_count").rename(columns={'9amto11am':'cell_id'})
    time_11to13=data.groupby(['call_start_date','11amto13pm'])['call_originating_number'].count().reset_index(name="11to13_count").rename(columns={'11amto13pm':'cell_id'})
    time_13to15=data.groupby(['call_start_date','13pmto15pm'])['call_originating_number'].count().reset_index(name="13to15_count").rename(columns={'13pmto15pm':'cell_id'})
    time_15to17=data.groupby(['call_start_date','15pmto17pm'])['call_originating_number'].count().reset_index(name="15to17_count").rename(columns={'15pmto17pm':'cell_id'})
    time_17to19=data.groupby(['call_start_date','17pmto19pm'])['call_originating_number'].count().reset_index(name="17to19_count").rename(columns={'17pmto19pm':'cell_id'})
    time_19to21=data.groupby(['call_start_date','19pmto21pm'])['call_originating_number'].count().reset_index(name="19to21_count").rename(columns={'19pmto21pm':'cell_id'})

    return time_7to9,time_9to11,time_11to13,time_13to15,time_15to17,time_17to19,time_19to21
    
#Get location of celltower at gvh level
def getlocation(data,malawi_gvh):
    for i,row in data.iterrows():
        pt=Point(float(data.loc[i,'long']),float(data.loc[i,'lat']))
        for a in malawi_gvh.index:
            polygon=malawi_gvh.loc[a,'geometry']
            if polygon.contains(pt):
                data.loc[i,'GVH']=malawi_gvh.loc[a,'NAME_3']
                data.loc[i,'TA']=malawi_gvh.loc[a,'NAME_2']
                data.loc[i,'DISTRICT']=malawi_gvh.loc[a,'NAME_1']
                #location=geolocator.reverse((float(data.loc[i,'lat']),float(data.loc[i,'long'])),timeout=60)
                #data.loc[i,'ADDRESS']= location.address
    return data

#For each day get the maximum used tower for each time window at each TA level
def get_max_each_window(data):
    out=pd.DataFrame()
    l=['7to9_count', '9to11_count','11to13_count', '13to15_count', '15to17_count', '17to19_count','19to21_count']
    for c in l:
        res=data.sort_values([c],ascending=False).groupby(['call_start_date','TA'],as_index=False).first()                
        for i,row in res.iterrows():
            out.loc[i,'Date']=res.loc[i,'call_start_date']
            out.loc[i,'District']=res.loc[i,'DISTRICT']
            out.loc[i,'TA']=res.loc[i,'TA']
            out.loc[i,'GVH_{}'.format(c)]=str(res.loc[i,'GVH']+'-'+res.loc[i,'cell_id']+'-'+str(res.loc[i,c]))
            #out_weekend.loc[i,'count_{}'.format(c)]=res.loc[i,c]
            #out_weekend.loc[i,'celltower_{}'.format(c)]=res.loc[i,'cell_id']
            #out_weekend.loc[i,'lat_{}'.format(c)]=res.loc[i,'lat']
            #out_weekend.loc[i,'long_{}'.format(c)]=res.loc[i,'long']
    return out
    
    
if __name__ == "__main__":
    
    pyodbc.autocommit = True 
    con = pyodbc.connect("DSN=malawihiveodbc", autocommit=True) 
    cursor = con.cursor()
    
    # Directory where embedding files are stored
    dir_in_shp="C:/MWI_adm/"
    
    #shapefilename
    file="MWI_adm3"
    
    #tablenames to be read
    table1="short_term_pop_movement_rainy_season"
    table2="short_term_pop_movement_non_rainy_season"
    table3="short_term_pop_movement_weekend"
    table4="cell_id_withoutlocation"
    
    #reading shapefile
    malawi_gvh=read_shape_data_file(dir_in_shp,file)
    
    #reading tables
    rainy=readdatahive(con,table1)
    nonrainy=readdatahive(con,table2)
    weekend=readdatahive(con,table3)
    tower_latlong=readdatahive(con,table4)
    
    col=['call_originating_number','7amto9am','9amto11am','11amto13pm','13pmto15pm',
                    '15pmto17pm','17pmto19pm','19pmto21pm','call_start_date']
    
    rainy.columns=col
    nonrainy.columns=col
    weekend.columns=col
    
    tower_latlong.columns=['sl', 'cell_id','lat','long','type','name_0','name_1','name_2']
    tower_latlong=tower_latlong[['cell_id','lat','long']]
    
    #Get count of originating numbers for each time window for each season
    rainy_7to9,rainy_9to11,rainy_11to13,rainy_13to15,rainy_15to17,rainy_17to19,rainy_19to21=time_groupby(rainy)
    nonrainy_7to9,nonrainy_9to11,nonrainy_11to13,nonrainy_13to15,nonrainy_15to17,nonrainy_17to19,nonrainy_19to21=time_groupby(nonrainy)
    wkend_7to9,wkend_9to11,wkend_11to13,wkend_13to15,rainy_15to17,rainy_17to19,rainy_19to21=time_groupby(weekend)
    
    #merge all data for different time windows for each season
    dfs = [rainy_7to9,rainy_9to11,rainy_11to13,rainy_13to15,rainy_15to17,rainy_17to19,rainy_19to21]
    df_rainy = reduce(lambda left,right: pd.merge(left,right,on=['call_start_date','cell_id'],how='outer'), dfs)
    
    df1= [nonrainy_7to9,nonrainy_9to11,nonrainy_11to13,nonrainy_13to15,nonrainy_15to17,nonrainy_17to19,nonrainy_19to21]
    df_nonrainy = reduce(lambda left,right: pd.merge(left,right,on=['call_start_date','cell_id'],how='outer'), df1)
    
    df2=[wkend_7to9,wkend_9to11,wkend_11to13,wkend_13to15,rainy_15to17,rainy_17to19,rainy_19to21]
    df_weekend = reduce(lambda left,right: pd.merge(left,right,on=['call_start_date','cell_id'],how='outer'), df2)
    
    df_rainy=df_rainy.fillna(0)
    df_nonrainy=df_nonrainy.fillna(0)
    df_weekend=df_weekend.fillna(0)
    
    #merge with tower file to get latitude and longitude 
    df_rainy_loc=pd.merge(df_rainy,tower_latlong,on='cell_id',how='inner')
    df_nonrainy_loc=pd.merge(df_nonrainy,tower_latlong,on='cell_id',how='inner')
    df_weekend_loc=pd.merge(df_weekend,tower_latlong,on='cell_id',how='inner')
    
    #Get location at gvh level using latlong TA
    df_rainy_f=getlocation(df_rainy_loc,malawi_gvh)
    df_nonrainy_f=getlocation(df_nonrainy_loc,malawi_gvh)
    df_wkend_f=getlocation(df_weekend_loc,malawi_gvh)
    
    #Get max subscriber get accumulated in each time window
    output_rainy=get_max_each_window(df_rainy_f)
    output_nonrainy=get_max_each_window(df_nonrainy_f)
    output_wkend=get_max_each_window(df_wkend_f)