# -*- coding: utf-8 -*-
"""
Created on Thu Mar 14 15:13:00 2019
After Clustering this module will caluclate the Probable points of cluster having
max population in the cluster.
"""

import geopandas as gpd
import pandas as pd

def Prob_Points(Malawi_Map,Malawi_cluster,Year,crs={'init': 'epsg:4326'}):
    Malawi_f=gpd.overlay(Malawi_Map,Malawi_cluster,how='intersection')    
    Probable_pooints=Malawi_f.sort_values(Year+'_pop_1_1',ascending=False).groupby('cluster_id',as_index=False).first()    
    Probable_pooints=gpd.GeoDataFrame(Probable_pooints,geometry='geometry')
    Probable_pooints['Centroid']=Probable_pooints.geometry.centroid
    Probable_pooints=Probable_pooints[['cluster_id', 'Country', 'District', 'TA_NAMES','2020_pop_1_1', '2021_pop_1_1', '2022_pop_1_1', '2023_pop_1_1', '2020_pop_1_2', '2021_pop_1_2', '2022_pop_1_2', '2023_pop_1_2', 'Centroid']]
    Probable_pooints.rename(columns={'2020_pop_1_1':'2020_pop_grid', '2021_pop_1_1':'2021_pop_grid', '2022_pop_1_1':'2022_pop_grid', '2023_pop_1_1':'2023_pop_grid', '2020_pop_1_2':'2020_pop_1', '2021_pop_1_2':'2021_pop_1', '2022_pop_1_2':'2022_pop_1', '2023_pop_1_2':'2023_pop_1'},inplace=True)
    Probable_pooints=gpd.GeoDataFrame(Probable_pooints,geometry='Centroid')
    Probable_pooints.crs=crs
    return Probable_pooints