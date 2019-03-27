# -*- coding: utf-8 -*-
"""
Created on Fri Mar 15 11:58:59 2019
This Module is used to calculate variale Radius on the Geometric Points.
"""

import geopandas as gpd
import pandas as pd
import numpy as np
import geog
from shapely.geometry import Point, Polygon

def Catchment(facilities,Malawi_pop,crs={'init': 'epsg:4326'}):
    facilities=facilities.dissolve(by='Inter_cir',as_index=False)
    covered_pop_2020= gpd.tools.sjoin(Malawi_pop,facilities,how='inner',op='intersects',lsuffix='map',rsuffix='fac')
    covered_pop_dissolve=covered_pop_2020[['Inter_cir','2020_pop_1', '2021_pop_1', '2022_pop_1', '2023_pop_1','geometry']]
    covered_pop_dissolve=gpd.GeoDataFrame(covered_pop_dissolve,crs=crs,geometry='geometry')
    covered_pop_dissolve=covered_pop_dissolve.dissolve(by='Inter_cir',aggfunc='sum',as_index=False)
    facilities=pd.merge(facilities,covered_pop_dissolve.drop('geometry',axis=1),on='Inter_cir')
    return facilities,covered_pop_2020

def Variable_Radius(facilities,Malawi_pop,Max_Radius,Initial_Radius,Increment_Radius,Max_pop_in_circle,Year,crs={'init': 'epsg:4326'}):
    facilities.crs=crs
    Max_Radius=Max_Radius+Increment_Radius
    facilities['radius']=Initial_Radius
    Flag=False
    columns=facilities.columns
    while True:          
        facilities['cvrd_poly']=facilities.apply(lambda row: Polygon(geog.propagate(row['geometry'],np.linspace(0,360,20),row['radius']*1000)),axis=1)
        facilities=facilities.set_geometry('cvrd_poly')
        covered_pop_2020= gpd.tools.sjoin(Malawi_pop,facilities,how='inner',op='intersects',lsuffix='map',rsuffix='fac')
        covered_pop_dissolve=covered_pop_2020[['dvcl','2020_pop_1', '2021_pop_1', '2022_pop_1', '2023_pop_1','geometry_map']]
        covered_pop_dissolve=gpd.GeoDataFrame(covered_pop_dissolve,crs=crs,geometry='geometry_map')
        covered_pop_dissolve=covered_pop_dissolve.dissolve(by='dvcl',aggfunc='sum',as_index=False)
        if Flag==True:
            facilities=pd.merge(facilities,covered_pop_dissolve.drop('geometry_map',axis=1),on='dvcl',how='left')
            Flag=False
            break
        check=pd.merge(facilities,covered_pop_dissolve.drop('geometry_map',axis=1),on='dvcl',how='left')    
        check['radius']=check.apply(lambda row: row['radius']+Increment_Radius if row[Year+'_pop_1']<=Max_pop_in_circle else row['radius'],axis=1)
        if check['radius'].max()==Max_Radius:
            facilities=pd.merge(facilities,covered_pop_dissolve.drop('geometry_map',axis=1),on='dvcl',how='left')
            facilities['radius']=facilities.apply(lambda row:row['radius']-Increment_Radius if row[Year+'_pop_1']>Max_pop_in_circle else row['radius'],axis=1)
            facilities=facilities[columns]
            Flag=True
            continue
        check=check[facilities.columns]
        facilities=check
    return facilities       
