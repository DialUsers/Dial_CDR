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
import Voronoi.vor_tes_boundary as vor_bound


##Get intersection of Voronoi and TA#####################################       

#Plot partially intersected and fully intersected voronoi tessaltions
def plotinter_areaTA(d,boundary,malawi_shp,tower_details,tower_year,dir_vor_shp):
    
    #Get voronoi shapefile for TA
    a=vor_bound.eachTA(d,boundary,malawi_shp,tower_details,tower_year,dir_vor_shp)
    #Assign the crs to the shapefile
    a.crs={'init': 'epsg:4326'}
    
    #Get the latitude and longitude for cell towers for perticular year
    latlong_tower=pd.merge(tower_details,tower_year,how='inner',on='cell_id')
    latlong_tower['geometry'] = list(zip(latlong_tower.LONG, latlong_tower.LAT))
    latlong_tower['geometry'] = latlong_tower['geometry'].apply(Point)
    latlong_tower = gpd.GeoDataFrame(latlong_tower, geometry='geometry')
    #Assign the crs to the shapefile
    latlong_tower.crs={'init': 'epsg:4326'}
    
    #Get the TA name for which we are getting voronoi
    TA=malawi_shp.loc[malawi_shp['NAME_2']==d]
    
    #overlaying the TA shape and voronoi shape and get intersection
    intersect = gpd.overlay(TA,a, how='intersection')
    
    #Get the voronoi which are completely inside the TA
    within=gpd.sjoin(a,intersect, how='inner',op='within')
#    ax=intersect.plot()
#    TA.plot(ax=ax, facecolor='none',color='red', edgecolor='white')
#    within.plot(ax=ax,color='blue')
#    a.plot(ax=ax, facecolor='none', edgecolor='k')
#    tower_in=gpd.sjoin(latlong_tower,a, how='inner',op='within')
#    tower_in.plot(color='black', ax=ax,markersize=2)
#    plt.savefig('D:\\Dial_codes\\plots\\voronoi_2016\\'+d+'_voronoi.png')
    
    '''To print no of polygons'''
    print('Total Number of polygons: '+str(a.shape[0]))
    print('Number of polygons intersecting partially: '+str(intersect.shape[0]))
    print('Number of polygons within district: '+str(within.shape[0]))
    
    return a





