
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
import Voronoi.vor_tes_boundary as vor_bound
import Voronoi.vor_adm_intersection as intersect

# read shapefile
def read_shape_data_file(dir_shp_read, filename):
    location = dir_shp_read + filename + '.shp'
    data=gpd.read_file(location)
    return data


#Get Area of intersection in KM**2
def area_TA(d,boundary,proj,malawi_shp,tower_details,tower_year,dir_vor_shp):
    # Get voronoi shapefile for TA
    a=intersect.plotinter_areaTA(d,boundary,malawi_shp,tower_details,tower_year,dir_vor_shp)
    
    #Get the latitude and longitude for cell towers for perticular year
    latlong_tower=pd.merge(tower_details,tower_year,how='inner',on='cell_id')
    latlong_tower['geometry'] = list(zip(latlong_tower.LONG, latlong_tower.LAT))
    latlong_tower['geometry'] = latlong_tower['geometry'].apply(Point)
    latlong_tower = gpd.GeoDataFrame(latlong_tower, geometry='geometry')
    
    #assign the crs
    latlong_tower.crs={'init': 'epsg:4326'}
    
    #Get the TA name for which we are getting voronoi 
    TA=malawi_shp.loc[malawi_shp['NAME_2']==d]
    
    #Change the crs of shapefile
    a = a.to_crs({'init': proj})
    latlong_tower_new=latlong_tower.to_crs({'init':proj})
    
    #Get the shape of towers with each voronoi
    final=gpd.sjoin(a,latlong_tower_new, how='inner')
    TA = TA.to_crs({'init': proj})
    
    #Get the overlap area of each TA and voronoi with celltower
    overlap_area = gpd.overlay(TA,final, how='intersection')
    overlap_area['Area_intersection']=overlap_area.geometry.area/10**6
    overlapping_area=overlap_area[['NAME_1_1','NAME_2_1','cell_id','Area_intersection']]
    
    # read shapefile of voronoi 
    filename=d+'vorbounded'
    voronoi_poly=read_shape_data_file(dir_vor_shp,filename)
    voronoi_poly.crs={'init': 'epsg:4326'}
    
    #change the crs
    voronoi_poly = voronoi_poly.to_crs({'init': 'epsg:32736'})
    
    #Get voronoi area for each celltower
    vor_area=gpd.sjoin(voronoi_poly,latlong_tower_new, how='inner')
    vor_area['Voronoi_Area']=vor_area.geometry.area/10**6
    vor_area_final=vor_area[['NAME_1','NAME_2','cell_id','Voronoi_Area']]
    
    #Get voronoi area and overlapping area of voronoi and TA for each celltower
    consolidated=pd.merge(vor_area_final,overlapping_area,on='cell_id',how='outer')
    
    return consolidated