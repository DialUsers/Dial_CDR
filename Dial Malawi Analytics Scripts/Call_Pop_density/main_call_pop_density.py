# -*- coding: utf-8 -*-
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import geopandas as gpd
from shapely.geometry import Point, LineString, Polygon, shape
from scipy.spatial import voronoi_plot_2d,Voronoi
import matplotlib as mpl
import matplotlib.cm as cm
import scipy as sp
import scipy.spatial
import sys
import os
from pyproj import Proj, transform
import Voronoi.vorarea_intersection_total as vorarea

#Read excel file
def read_data(dir,filename):
    location = dir + filename + '.xlsx'
    df_file = pd.read_excel(location)
    return df_file

#Read csv file
def readcsv(dir,filename):
    location = dir + filename + '.csv'
    df_file = pd.read_csv(location)
    return df_file

#Write excel file
def write_data(dir,filename,df_write):
    location = dir + filename + '.csv'
    df_write.to_excel(location)
    return

#Read shapefile
def read_shape_data_file(dir_shp_read, filename):
    location = dir_shp_read + filename + '.shp'
    data=gpd.read_file(location)
    return data
    
#Write shapefile
def write_shape_data_file(dir_shp_write, filename, df_write):
    location = dir_shp_write + filename + '.shp'
    df_write.to_file(location)
    return

#For Each TA get call_density and population
def get_call_pop_density_TA(boundary,proj,malawi_shp,tower_details,tower_year,cdr_input,
                            worldpop_TA,year,dir_shp_write,shpfileout,dir_shp_vor):
    
    #Get the coordinates of towers which are there in cdr data from tower master
    latlong_tower=pd.merge(tower_details,tower_year,how='inner',on='cell_id')
    latlong_tower['geometry'] = list(zip(latlong_tower.LONG, latlong_tower.LAT))
    latlong_tower['geometry'] = latlong_tower['geometry'].apply(Point)
    latlong_tower = gpd.GeoDataFrame(latlong_tower, geometry='geometry')
    latlong_tower.crs={'init': 'epsg:4326'}
    
    ###Getting cell_id and unique subscriber for that tower#################
    malawi_worldpop=pd.merge(malawi_shp,worldpop_TA,how='inner',left_on=['NAME_1','NAME_2'],right_on=['District','TA'])
    
    ###Getting 2016 pop estimation by using growth rate and using 2015 worlpop data##################################
    malawi_worldpop['{}_pop'.format(year)]=malawi_worldpop['Sum (ppp) adjusted']*(2.718**((year-2015)*malawi_worldpop['2020-2015 growth rate']))
    malawi_worldpop=malawi_worldpop[['District.1', 'TA', 'Zone', 'Sum (ppp) adjusted','2017_pop','geometry']]
    malawi_worldpop.columns=['District', 'TA', 'Zone', 'Sum (ppp) adjusted','{}_pop'.format(year),'geometry']

    call_density=pd.DataFrame(columns=['District','TA','Call_density'])
    
    ###Getting call density for each TA ############################################
    
    for i,row in malawi_worldpop.iterrows():
#        print(i)
        try:
            d=malawi_worldpop.loc[i,'TA']
            dis=malawi_worldpop.loc[i,'District']
            b=vorarea.area_TA(d,boundary,proj,malawi_shp,tower_details,tower_year,dir_vor_shp)
            malawi_area = malawi_worldpop.to_crs({'init': 'epsg:32736'})
            b['Total_TA_Area']=malawi_area.geometry.area[i]/10**6
            matching=pd.merge(b,cdr_input, how='inner',on='cell_id')
            for i,row in matching.iterrows():
                matching.loc[i,'Tower_call_density']=matching.loc[i,'sum_unique']/matching.loc[i,'Voronoi_Area']
                matching.loc[i,'Dv_intersection']=matching.loc[i,'Tower_call_density']*matching.loc[i,'Area_intersection']
                
            Total_call_density_TA=matching['Dv_intersection'].sum()
            call_density=call_density.append({'District':dis,'TA':d,'Call_density':Total_call_density_TA},ignore_index=True)
#            print(i)
        except (ValueError,IndexError) as e:
            pass
    
    ###Get call_density, population_density for each TA to go ahead with spatial regression############################
    
    malawi_pcd=pd.merge(malawi_worldpop,call_density,how='inner',on=['District','TA'])
    malawi_pcd=malawi_pcd[['District', 'TA', 'Zone', 'Sum (ppp) adjusted', '2017_pop','Call_density','geometry']]
    malawi_pcd = malawi_pcd.to_crs({'init':'epsg:32736'})
    malawi_pcd['Total_TA_area']=malawi_pcd['geometry'].area/10**6
    
    write_shape_data_file(dir_shp_write,shpfileout,malawi_pcd)
    
    return

if __name__ == "__main__":
    
    # Configuration
    boundary=0.09
    proj='epsg:32736'
    
    # Directory where embedding files are stored
    dir_in='D:/Dial/'
    dir_in_shp='D:/Dial/MWI_adm/'
    dir_out_shp='D:/Dial_codes/call_pop_density_shapefile/'
    dir_vor_shp='D:/Dial_codes/voronoi_shapefiles/'
    
    # filenames which are required
    shpfileadm2='MWI_adm2'
    towerfile='latlong_withTA'
    tower2016='cellid_2016'
    tower2017='cellid_2017'
    cdr_2016='sum_unique_2016'
    cdr_2017='sum_unique_2017'
    adm2wpop='adm2_wisepop'
    shpfileout_2016='malawi_pcd_TA_2016'
    shpfileout_2017='malawi_pcd_TA_2017'
    
    # read the inputs
    malawi_shp=read_shape_data_file(dir_in_shp,shpfileadm2)
    tower_details=read_data(dir_in,towerfile)
    tower_year=readcsv(dir_in,tower2016)
    cdr_input=readcsv(dir_in,cdr_2016)
    worldpop_TA=read_data(dir_in,adm2wpop)
    year=2016
    
    # call the function to calculate the distance
    get_call_pop_density_TA(boundary,proj,malawi_shp,tower_details,tower_year,cdr_input,worldpop_TA,
                            year,dir_out_shp,shpfileout_2016,dir_vor_shp)
    

    
    
    
