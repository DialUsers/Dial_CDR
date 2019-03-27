# -*- coding: utf-8 -*-
"""
Created on Wed Mar 27 11:19:03 2019
This Module is used for the Demand Points Selection
"""

import pandas as pd
import numpy as np
import geopandas as gpd
import pyproj
from shapely.geometry import Point, LineString, Polygon, shape, multipolygon


def demand_Points(Uncover_TA, Probable_Points, GVH_ADM, Covered_Areas, Rank_Min, Cluster_Min, Year, Disease_Flag):
    if Disease_Flag==False:
        Uncover_TA.rename(columns={'2020_pop_1':'2020_TA_Uncover','2021_pop_1':'2021_TA_Uncover','2022_pop_1':'2022_TA_Uncover','2023_pop_1':'2023_TA_Uncover'}, inplace=True)
        Uncover_TA['TA_Rank'] = Uncover_TA[Year+'_TA_Uncover'].rank(axis=0, ascending=True, pct=False)
        Uncover_Probable_Points_Ranks = pd.merge(Probable_Points, Uncover_TA, on=['Country', 'District', 'TA_NAMES'],how='inner')
    elif Disease_Flag==True:
        Probable_Points['District_new']=Probable_Points['District'].str.upper()
        Probable_Points['TA_NAMES_new']=Probable_Points['TA_NAMES'].str.upper()
        Uncover_Probable_Points_Ranks = pd.merge(Probable_Points, Uncover_TA, on=['District_new', 'TA_NAMES_new'],how='inner')
    Prob_GVH_Unc_Rank = gpd.tools.sjoin(Uncover_Probable_Points_Ranks, GVH_ADM, how='left', op='intersects')
    Prob_GVH_Unc_Rank.drop('index_right',axis=1,inplace=True)
    Check_Points=gpd.tools.sjoin(Prob_GVH_Unc_Rank, Covered_Areas[['geometry']],how='left',op='within')
    Uncover_GVH_Rank_Points=Check_Points[Check_Points['index_right'].isnull()]    
    if Disease_Flag==False:
        Demand_Points_selct = Uncover_GVH_Rank_Points[(Uncover_GVH_Rank_Points['TA_Rank'] > Rank_Min) & (Uncover_GVH_Rank_Points[Year+'_pop_1'] > Cluster_Min)].copy()
    elif Disease_Flag==True:
        Demand_Points_selct = Uncover_GVH_Rank_Points[(Uncover_GVH_Rank_Points['TA_Rank'] < Rank_Min) & (Uncover_GVH_Rank_Points[Year+'_pop_1'] > Cluster_Min)].copy()        
    Demand_Points_selct['D_TA'] = Demand_Points_selct['District'] + '_' + Demand_Points_selct['TA_NAMES']
    Demand_Points_selct.reset_index(drop=True, inplace=True)
    Demand_Points_selct.reset_index(inplace=True)
    Demand_Points_selct.rename(columns={'NAME_3' : 'GVH', 'index' : 'dvcl'}, inplace=True)
    Demand_Points_selct = Demand_Points_selct[['Country','District','TA_NAMES','cluster_id', 'GVH', 'D_TA', 'geometry', 'dvcl']].copy()
    return Demand_Points_selct
