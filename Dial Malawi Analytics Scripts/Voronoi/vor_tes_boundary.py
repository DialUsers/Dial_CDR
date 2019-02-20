'''GET THE VORONOI TESSALATIONS FOR EACH TA(ADM2) USING CELL TOWERS AND 
CDR DATA FOR EACH YEAR 2016 AND 2017'''


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

#write shapefile
def write_shape_data_file(dir_shp_write, filename, df_write):
    location = dir_shp_write + filename + '.shp'
    df_write.to_file(location)
    return

##Creating Voronoi with Bound each TA level##################################################
def eachTA(d,boundary,malawi_shp,tower_details,tower_year,dir_vor_shp):
    
    # Get latitude and longitude of celltower for each year from master
    latlong_tower=pd.merge(tower_details,tower_year,how='inner',on='cell_id')
    
    #Get TA name for which we are plotting voronoi
    TA=malawi_shp.loc[malawi_shp['NAME_2']==d]
    
    vor=latlong_tower[['LONG','LAT']]

    #Getting the TA min and max bounds i.e 10KM boundary across TA '''
    min_x=TA.bounds.iloc[0][0]-boundary
    min_y=TA.bounds.iloc[0][1]-boundary
    max_x=TA.bounds.iloc[0][2]+boundary
    max_y=TA.bounds.iloc[0][3]+boundary
    bounding_box = np.array([min_x, max_x, min_y, max_y])
    tower=vor.values

    
    #Get the towers which are inside the boundary for perticular TA
    def in_box(tower, bounding_box):
        return np.logical_and(np.logical_and(bounding_box[0] <= tower[:, 0],
                                     tower[:, 0] <= bounding_box[1]),
                      np.logical_and(bounding_box[2] <= tower[:, 1],
                                     tower[:, 1] <= bounding_box[3]))
    
    #Plot voronoi tessalations and create shapefile for each voronoi
    def voronoi(tower, bounding_box):
        # Select towers inside the bounding box
        i = in_box(tower, bounding_box)
        # Mirror points
        points_center = tower[i, :]
        points_left = np.copy(points_center)
        points_left[:, 0] = bounding_box[0] - (points_left[:, 0] - bounding_box[0])
        points_right = np.copy(points_center)
        points_right[:, 0] = bounding_box[1] + (bounding_box[1] - points_right[:, 0])
        points_down = np.copy(points_center)
        points_down[:, 1] = bounding_box[2] - (points_down[:, 1] - bounding_box[2])
        points_up = np.copy(points_center)
        points_up[:, 1] = bounding_box[3] + (bounding_box[3] - points_up[:, 1])
        points = np.append(points_center,
                           np.append(np.append(points_left,
                                               points_right,
                                               axis=0),
                                     np.append(points_down,
                                               points_up,
                                               axis=0),
                                     axis=0),
                           axis=0)
        # Compute Voronoi
        v = sp.spatial.Voronoi(points)
        regions = []
        for region in v.regions:
            flag = True
            for index in region:
                if index == -1:
                    flag = False
                    break
                else:
                    x = v.vertices[index, 0]
                    y = v.vertices[index, 1]
                    if not(bounding_box[0] <= x and x <= bounding_box[1] and
                           bounding_box[2] <= y and y <= bounding_box[3]):
                        flag = False
                        break
            if region != [] and flag:
                regions.append(region)
        v.filtered_points = points_center
        v.filtered_regions = regions
        return v
    
    new_vor = voronoi(tower, bounding_box)
    
    voronoipolys=[]
    for i in range(len(np.unique(new_vor.filtered_points,axis=0))):
        a=new_vor.vertices[new_vor.filtered_regions[i]]
        voronoipolys.append((new_vor.filtered_points[i],[tuple(l) for l in a]))
    poligons = [Polygon(voronoipolys[i][1]) for i in range(len(voronoipolys))]
    poly_df = gpd.GeoDataFrame(geometry=poligons)
    
    #write shapefile of voronoi to use it later to get area intersection of voronoi and TA
    outfile= d+'vorbounded'
    write_shape_data_file(dir_vor_shp,outfile,poly_df)

    return poly_df