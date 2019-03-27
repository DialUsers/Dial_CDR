# -*- coding: utf-8 -*-
"""
Created on Thu Mar 14 16:08:57 2019
This module includes the Extra Function required for Model 1.
"""

## Finding Uncovered Population for Next year
import pandas as pd
import geopandas as gpd
import numpy as np
import PHASE2_PACKAGE.Overlapping_Areas as Overlp

## Finding Uncovered Population for Next year
def Map_Unc(Malawi_pop,covered_pop):
    Malawi_Map=gpd.overlay(Malawi_pop,covered_pop,how='difference')
    return Malawi_Map

## Finding TA Wise Uncover Population.
def TA_Wise_Uncover(Malawi_pop):
    Malawi_TA=Malawi_pop.groupby(['Country','District','TA_NAMES'],as_index=False).agg({'2020_pop_1':'sum', '2021_pop_1':'sum', '2022_pop_1':'sum', '2023_pop_1':'sum'}).reset_index()
    return Malawi_TA

## Finding the covered Area every year.	
def Inc_Covered(covered_area, covered_pop):
    covered=gpd.overlay(covered_pop,covered_area,how='union')
    return covered

## Choosing the rows for selecting the health post in the overlapping catchment areas of health post locations.
def choose_rows(facilities,fc_bins,Year):
	facilities_f=pd.DataFrame()
	for ind in fc_bins.index:
		facilities_over=facilities[(facilities['Count_Inter_cir']>=fc_bins.at[ind,'Min']) & (facilities['Count_Inter_cir']<=fc_bins.at[ind,'Max'])]
		facilities_over=facilities_over.sort_values(Year+'_pop_1',ascending=False).groupby('Inter_cir',as_index=False).head(fc_bins.at[ind,'Records'])
		facilities_f=pd.concat([facilities_over,facilities_f],axis=0)
	facilities_f=facilities_f.reset_index(drop=True).copy()
	facilities_f.drop(['Inter_cir','Count_Inter_cir'],axis=1,inplace=True)
	facilities_f=Overlp.Overlapping_Areas(facilities_f,crs={'init': 'epsg:4326'})
	Count_Inter=facilities_f.groupby('Inter_cir',as_index=False).size().reset_index(name='Count_Inter_cir')
	facilities_f=pd.merge(facilities_f, Count_Inter, on='Inter_cir')
	return facilities_f

## Finding the catchment population of health post facilities.
def catchment_pop(facilities, facilities_overlapped, Year):
    column=list(facilities.columns)
    Total_values=facilities.groupby('Inter_cir',as_index=False).agg({'radius':'sum','2020_pop_1':'sum', '2021_pop_1':'sum', '2022_pop_1':'sum', '2023_pop_1':'sum'})
    Total_values.rename(columns={'radius':'Tot_rad'},inplace=True)
    Total_values=pd.merge(Total_values,facilities_overlapped.drop('geometry',axis=1),on='Inter_cir',suffixes=('_tot','_merge'))
    facilities=pd.merge(facilities,Total_values,how='left',on='Inter_cir')
    facilities[Year+'_pop_catch']=facilities.apply(lambda row: row[Year+'_pop_1']-((row[Year+'_pop_1_tot']-row[Year+'_pop_1_merge'])*(row['radius']/row['Tot_rad'])) if row['Count_Inter_cir'] > 1 else row[Year+'_pop_1'], axis=1)
    facilities=facilities[[Year+'_pop_catch']+column]
    return facilities