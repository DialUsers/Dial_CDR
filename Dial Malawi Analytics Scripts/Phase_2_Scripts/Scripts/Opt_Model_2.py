# -*- coding: utf-8 -*-
"""
Created on Thu Mar 14 11:47:38 2019
This module is used for finding the uncovered pop in the Model 2.
"""

import numpy as np
import pandas as pd
import geopandas as gpd
from scipy.spatial import cKDTree
import itertools
import os
import math

# Set default path to PHASE 2 PACKAGES
os.chdir("E:/Pankaj_Malawi_Shape_files/Code/")


## Importing the Custom Build Package
import PHASE2_PACKAGE.Cluster_Creation as cluster 
import PHASE2_PACKAGE.Prob_Clus_Max_Points as Prob
import PHASE2_PACKAGE.Shape_Read_Write as shp
import PHASE2_PACKAGE.Merge_Row_DataFrame as Mo_Ro
import PHASE2_PACKAGE.Variable_Radius as Varib 
import PHASE2_PACKAGE.Overlapping_Areas as Overlp
import PHASE2_PACKAGE.Extra as Extra
import PHASE2_PACKAGE.Pandas_Read_Write as pds
import PHASE2_PACKAGE.Demand_Points as dp

# Initialize the Variable whether we are using the Disease Ranking or not 
Disease_Flag=True

# Reading the GVH Adminstrator File
GVH_Dir="F:/Inputs/MWI_adm/"
GVH_ADM_Name='MWI_adm3'
GVH_ADM=shp.read_shape_data_file(GVH_Dir,GVH_ADM_Name)

# Reading Weights without Disease Ranking
Dis_Dir="F:/Inputs/"
Dis_Rank_Name='TA_Weights_Without_Disease_Ranking'
Dis_Rank=pds.read_csv_data_file(Dis_Dir,Dis_Rank_Name)

################### Model 2 Year 1 Attriburtes and Tables ###########################

# Model 2 Paths for the Input files
Dir_Year_1="F:/Model_2/Year_1/"
Malawi_Map_Name_Year_1="Malawi_Distribution"

# choosing variables for the Demand Points
Rank_Min_Year_1=180
Cluster_Min_Year_1=8000

# Various Attributes for the Model 2
crs={'init': 'epsg:4326'}
Year_1='2020'
Pop_in_Clus_Year_1=12000
near_point_Year_1=12
clus_size_Year_1=800

Max_Radius_Year_1=6
Initial_Radius_Year_1=0.5
Increment_Radius_Year_1=0.5
Max_pop_in_circle_Year_1=12500
Number_of_Health_Post_Year_1=198
# Various Attributes for Likoma Island for Model 2
near_point_Likoma_Year_1=2
clus_size_Likoma_Year_1=1

# Model 2 Paths for the Output files
Malawi_cluster_Name_Year_1="Malawi_cluster_"+Year_1+'_'+str(near_point_Year_1)
Malawi_cluster_Likoma_Name_Year_1="Malawi_cluster_Likoma_"+Year_1+'_'+str(near_point_Likoma_Year_1)
Malawi_cluster_Merge_Name_Year_1="Malawi_cluster_Merge_"+Year_1
Probable_Points_Name_Year_1="Probable_Points"+Year_1
Demand_Points_Name_Year_1="Demand_Points_"+Year_1
Health_Post_Name_Year_1="Health_Posts_"+Year_1
Health_Post_Overlapped_Name_Year_1="Health_Posts_Overlapped_"+Year_1
Covered_Grid_Name_Year_1="Covered_Grid_Area_"+Year_1
Covered_Areas_Name_Year_1="Overall_Covered_Area_"+Year_1

# Applying Logics
Malawi_Map_year_1=shp.read_shape_data_file(Dir_Year_1, Malawi_Map_Name_Year_1)

########## Extra Experimentation with removing National Park, Water body, Reserve Areas Comletely ###################
#Malawi_pop_Year_1=Malawi_Map[~Malawi_Map['TYPE_2'].isin(['National Park','Water body','Reserve'])]
#Malawi_uncover=Malawi_Map[Malawi_Map['TYPE_2'].isin(['National Park','Water body','Reserve'])]
#Malawi_uncover[Malawi_uncover['Type']=='Uncover']['2020_pop_1'].sum()
####################################################################################################################

# Selecting Uncover Areas for the Malawi Map for Year 1
Malawi_pop=Malawi_Map_year_1[Malawi_Map_year_1['Type']=='Uncover'].copy()

# drop Null values from the Map
Malawi_pop.dropna(inplace=True)

# Removing National Park, Water body, Reserve Areas having Population 0
Malawi_pop_Year_1=Malawi_pop[~(Malawi_pop['TYPE_2'].isin(['National Park','Water body','Reserve']) & (Malawi_pop[Year_1+'_pop_1']==0))]

# Removing Likoma District because it is seperate island from the Map Malawi
Malawi_pop_Year_1=Malawi_pop[Malawi_pop['District']!='Likoma']

# Removing the very small areas for creating clusters easy and where the population for the Year 1 is zero.
Malawi_pop_Year_1['Area']=Malawi_pop_Year_1.geometry.area
Malawi_pop_Year_1=Malawi_pop_Year_1[(Malawi_pop_Year_1['Area']>1.724197566456432E-08) & (Malawi_pop_Year_1[Year_1+'_pop_1']!=0)]

# Creating the clusters by passing parameters through the Function
Malawi_cluster_Year_1=cluster.Cluster_creation(Malawi_pop_Year_1,near_point_Year_1,clus_size_Year_1,Year_1,Pop_in_Clus_Year_1)

# For Likoma Island creating the clusters
Malawi_pop_Likoma_Year_1=Malawi_pop[Malawi_pop['District']=='Likoma']

# Creating the clusters for Likoma islands by passing parameters through the Function
Malawi_cluster_Likoma_Year_1=cluster.Cluster_creation(Malawi_pop_Likoma_Year_1,near_point_Likoma_Year_1,clus_size_Likoma_Year_1,Year_1,Pop_in_Clus_Year_1)

# Merge the Likoma Islands file with Malawi_Map 
Malawi_cluster_Merge_Year_1=Mo_Ro.Merge_Row_DataFrame(Malawi_cluster_Year_1, Malawi_cluster_Likoma_Year_1)

# Probable Points for Year 1 based on Max Grid population in the cluster and then choose centroid.
Probable_Points_Year_1 = Prob.Prob_Points(Malawi_pop,Malawi_cluster_Merge_Year_1,Year_1,crs)

# Saving the files of Probable Points, Malawi_cluster_Merge, Malawi_cluster_likoma, Malawi_cluster.
shp.write_shape_data_file(Dir_Year_1,Malawi_cluster_Name_Year_1,Malawi_cluster_Year_1)
shp.write_shape_data_file(Dir_Year_1,Malawi_cluster_Likoma_Name_Year_1,Malawi_cluster_Likoma_Year_1)
shp.write_shape_data_file(Dir_Year_1,Malawi_cluster_Merge_Name_Year_1,Malawi_cluster_Merge_Year_1)
shp.write_shape_data_file(Dir_Year_1, Probable_Points_Name_Year_1,Probable_Points_Year_1)

## Swati works Here.
## Selecting Demand Points from Probable Points.

Existing_Health_Post_Areas=Malawi_Map_year_1[Malawi_Map_year_1['Type']=='Cover'].copy()
Demand_Points_Year_1=dp.demand_Points(Dis_Rank, Probable_Points_Year_1, GVH_ADM, Existing_Health_Post_Areas, Rank_Min_Year_1, Cluster_Min_Year_1, Year_1, Disease_Flag)

## Saving Demand Points
shp.write_shape_data_file(Dir_Year_1,Demand_Points_Name_Year_1,Demand_Points_Year_1)
pds.write_csv_data_file(Dir_Year_1, Demand_Points_Name_Year_1, Demand_Points_Year_1)

## Sham Works here for Variable Radius Circle and Overlapping Circles.

Demand_Points_Year_1=shp.read_shape_data_file(Dir_Year_1, Demand_Points_Name_Year_1)

# For year 1 only
Malawi_pop['Area']=Malawi_pop.geometry.area
Malawi_pop=Malawi_pop[(Malawi_pop['Area']>1.724197566456432E-08) & (Malawi_pop[Year_1+'_pop_1']!=0)]

# Variable Circle Formation
Demand_Points_Catchment_Year_1=Varib.Variable_Radius(Demand_Points_Year_1, Malawi_pop, Max_Radius_Year_1,Initial_Radius_Year_1,Increment_Radius_Year_1,Max_pop_in_circle_Year_1,Year_1,crs)
Demand_Points_Catchment_Year_1=Demand_Points_Catchment_Year_1[['Country', 'District', 'TA_NAMES', 'cluster_id', 'GVH', 'D_TA', 'dvcl', 'radius', 'cvrd_poly', '2020_pop_1', '2021_pop_1', '2022_pop_1', '2023_pop_1']]

# Overlapping Circle Formation
Demand_Points_Catchment_Year_1.rename(columns={'cvrd_poly':'geometry'},inplace=True)
Demand_Points_Overlapped_Catchment_Year_1=Overlp.Overlapping_Areas(Demand_Points_Catchment_Year_1,crs={'init': 'epsg:4326'})

# Caluclating the count of Intersecting circles and merge it with Demand_Points_Overlapped_Catchment.
Count_Inter_Cir_Year_1=Demand_Points_Overlapped_Catchment_Year_1.groupby('Inter_cir',as_index=False).size().reset_index(name='Count_Inter_cir')
Demand_Points_Overlapped_Catchment_Year_1=pd.merge(Demand_Points_Overlapped_Catchment_Year_1, Count_Inter_Cir_Year_1, on='Inter_cir')

# calculate catchment Area and excluding those having areas is zero.
Demand_Points_Overlapped_Catchment_Year_1['Catchment_Area']=(Demand_Points_Overlapped_Catchment_Year_1['radius']**2)*math.pi 
Demand_Points_Overlapped_Catchment_Year_1=Demand_Points_Overlapped_Catchment_Year_1[Demand_Points_Overlapped_Catchment_Year_1['Catchment_Area']>0]

# From multiple points selecting according to CIP Number and taking first circle from overlapped.
Demand_Points_Overlapped_Catchment_Year_1=Demand_Points_Overlapped_Catchment_Year_1.sort_values(Year_1+'_pop_1',ascending=False).groupby('Inter_cir',as_index=False).first() 
Health_Post_Year_1=Demand_Points_Overlapped_Catchment_Year_1.sort_values(Year_1+'_pop_1',ascending=False).head(Number_of_Health_Post_Year_1)

# Convert the Health Post Datafrmae to Geopandas DataFrame and caluclates the catchment Population after that.
Health_Post_Year_1=gpd.GeoDataFrame(Health_Post_Year_1,geometry='geometry',crs=crs)
Health_Post_Overlapped_Year_1=Health_Post_Year_1[['Inter_cir','geometry']]
Health_Post_Overlapped_Year_1, Covered_Grid_Year_1 = Varib.Catchment(Health_Post_Overlapped_Year_1, Malawi_pop)

# Saving the Health Post Files
shp.write_shape_data_file(Dir_Year_1,Health_Post_Name_Year_1,Health_Post_Year_1)
shp.write_shape_data_file(Dir_Year_1,Health_Post_Overlapped_Name_Year_1,Health_Post_Overlapped_Year_1)
shp.write_shape_data_file(Dir_Year_1,Covered_Grid_Name_Year_1,Covered_Grid_Year_1)

# Calulate overall covered Area

# For 1st Year only
Covered_Areas_Year_1=Extra.Inc_Covered(Existing_Health_Post_Areas, Covered_Grid_Year_1)

# Saving Covered Area File
shp.write_shape_data_file(Dir_Year_1,Covered_Areas_Name_Year_1,Covered_Areas_Year_1)
################### Model 2 Year 1 End ################################################################



###################Model 2 Year 2 Starts #############################################################
Dir_Year_2="F:/Model_2/Year_2/"


# Various Attributes for the Model 2
crs={'init': 'epsg:4326'}
Year_2='2021'
Pop_in_Clus_Year_2=12000
near_point_Year_2=7
clus_size_Year_2=100
Malawi_Map_Name_Year_2="Malawi_Distribution_"+Year_2
Uncover_TA_Name_Year_2="Uncovered_TA_"+Year_2
Overlapped_bins_Name_Year_2="Overlapped_bins_"+Year_2

Max_Radius_Year_2=6
Initial_Radius_Year_2=0.5
Increment_Radius_Year_2=0.5
Max_pop_in_circle_Year_2=12500
Number_of_Health_Post_Year_2=234
# Various Attributes for Likoma Island for Model 2
near_point_Likoma_Year_2=2
clus_size_Likoma_Year_2=1

# choosing variables for the Demand Points
Rank_Min_Year_2=200
Cluster_Min_Year_2=8000

# Model 2 Paths for the Output files
Malawi_cluster_Name_Year_2="Malawi_cluster_"+Year_2+'_'+str(near_point_Year_2)
Malawi_cluster_Likoma_Name_Year_2="Malawi_cluster_Likoma_"+Year_2+'_'+str(near_point_Likoma_Year_2)
Malawi_cluster_Merge_Name_Year_2="Malawi_cluster_Merge_"+Year_2
Probable_Points_Name_Year_2="Probable_Points"+Year_2
Demand_Points_Name_Year_2="Demand_Points_"+Year_2
Health_Post_Name_Year_2="Health_Posts_"+Year_2
Health_Post_Overlapped_Name_Year_2="Health_Posts_Overlapped_"+Year_2
Covered_Grid_Name_Year_2="Covered_Grid_Area_"+Year_2
Covered_Areas_Name_Year_2="Overall_Covered_Area_"+Year_2

# Applying Logistics

# Calculating the New Map of Malawi excluding the health post that covered in previous year.
Malawi_Map_Year_2=Extra.Map_Unc(Malawi_pop,Covered_Areas_Year_1)
Malawi_Map_Year_2=Malawi_Map_Year_2[['Country', 'District', 'TA_NAMES', 'TYPE_2', 'Type', 'ENGTYPE_2',
       'Grid_index', '2018_pop_1', '2019_pop_1', '2020_pop_1', '2021_pop_1', '2022_pop_1',
       '2023_pop_1', 'geometry']]

# Calculating uncovered population according to TA Wise.
Uncover_TA_Year_2=Extra.TA_Wise_Uncover(Malawi_Map_Year_2)

# Saving the Malawi Map and Uncover TA Files.
shp.write_shape_data_file(Dir_Year_2,Malawi_Map_Name_Year_2, Malawi_Map_Year_2)
pds.write_csv_data_file(Dir_Year_2,Uncover_TA_Name_Year_2,Uncover_TA_Year_2)

# Selecting Uncover Areas for the Malawi Map for Year 1
Malawi_pop=Malawi_Map_Year_2[Malawi_Map_Year_2['Type']=='Uncover'].copy()

# drop Null values from the Map
Malawi_pop.dropna(inplace=True)

# Removing National Park, Water body, Reserve Areas having Population 0
Malawi_pop_Year_2=Malawi_pop[~(Malawi_pop['TYPE_2'].isin(['National Park','Water body','Reserve']) & (Malawi_pop[Year_2+'_pop_1']==0))]

# Removing Likoma District because it is seperate island from the Map Malawi
Malawi_pop_Year_2=Malawi_pop[Malawi_pop['District']!='Likoma']

# Removing the very small areas for creating clusters easy and where the population for the Year 1 is zero.
Malawi_pop_Year_2['Area']=Malawi_pop_Year_2.geometry.area
Malawi_pop_Year_2=Malawi_pop_Year_2[(Malawi_pop_Year_2['Area']>1.724197566456432E-08) & (Malawi_pop_Year_2[Year_2+'_pop_1']!=0)]

# Creating the clusters by passing parameters through the Function
Malawi_cluster_Year_2=cluster.Cluster_creation(Malawi_pop_Year_2,near_point_Year_2,clus_size_Year_2,Year_2,Pop_in_Clus_Year_2)

# For Likoma Island creating the clusters
Malawi_pop_Likoma_Year_2=Malawi_pop[Malawi_pop['District']=='Likoma']

# Creating the clusters for Likoma islands by passing parameters through the Function
Malawi_cluster_Likoma_Year_2=cluster.Cluster_creation(Malawi_pop_Likoma_Year_2,near_point_Likoma_Year_2,clus_size_Likoma_Year_2,Year_2,Pop_in_Clus_Year_2)
Malawi_cluster_Likoma_Year_2=Malawi_cluster_Likoma_Year_2[:1]

# Merge the Likoma Islands file with Malawi_Map 
Malawi_cluster_Merge_Year_2=Mo_Ro.Merge_Row_DataFrame(Malawi_cluster_Year_2, Malawi_cluster_Likoma_Year_2)

# Probable Points for Year 1 based on Max Grid population in the cluster and then choose centroid.
Probable_Points_Year_2 = Prob.Prob_Points(Malawi_pop,Malawi_cluster_Merge_Year_2,Year_2,crs)

# Saving the files of Probable Points, Malawi_cluster_Merge, Malawi_cluster_likoma, Malawi_cluster.
shp.write_shape_data_file(Dir_Year_2,Malawi_cluster_Name_Year_2,Malawi_cluster_Year_2)
shp.write_shape_data_file(Dir_Year_2,Malawi_cluster_Likoma_Name_Year_2,Malawi_cluster_Likoma_Year_2)
shp.write_shape_data_file(Dir_Year_2,Malawi_cluster_Merge_Name_Year_2,Malawi_cluster_Merge_Year_2)
shp.write_shape_data_file(Dir_Year_2, Probable_Points_Name_Year_2,Probable_Points_Year_2)

## Swati works Here.
## Selecting Demand Points from Probable Points.

Demand_Points_Year_2=dp.demand_Points(Dis_Rank, Probable_Points_Year_2, GVH_ADM, Covered_Areas_Year_1, Rank_Min_Year_2, Cluster_Min_Year_2, Year_2, Disease_Flag)

## Saving Demand Points
shp.write_shape_data_file(Dir_Year_2,Demand_Points_Name_Year_2,Demand_Points_Year_2)
pds.write_csv_data_file(Dir_Year_2, Demand_Points_Name_Year_2, Demand_Points_Year_2)

## Sham Works here for Variable Radius Circle and Overlapping Circles.

# Variable Circle Formation
Demand_Points_Year_2=shp.read_shape_data_file(Dir_Year_2, Demand_Points_Name_Year_2)

Demand_Points_Catchment_Year_2=Varib.Variable_Radius(Demand_Points_Year_2, Malawi_pop, Max_Radius_Year_2,Initial_Radius_Year_2,Increment_Radius_Year_2,Max_pop_in_circle_Year_2,Year_2,crs)
Demand_Points_Catchment_Year_2=Demand_Points_Catchment_Year_2[['Country', 'District', 'TA_NAMES', 'cluster_id', 'GVH', 'D_TA', 'dvcl', 'radius', 'cvrd_poly', '2020_pop_1', '2021_pop_1', '2022_pop_1', '2023_pop_1']]

# Overlapping Circle Formation
Demand_Points_Catchment_Year_2.rename(columns={'cvrd_poly':'geometry'},inplace=True)
Demand_Points_Overlapped_Catchment_Year_2=Overlp.Overlapping_Areas(Demand_Points_Catchment_Year_2,crs={'init': 'epsg:4326'})

# Caluclating the count of Intersecting circles and merge it with Demand_Points_Overlapped_Catchment.
Count_Inter_Cir_Year_2=Demand_Points_Overlapped_Catchment_Year_2.groupby('Inter_cir',as_index=False).size().reset_index(name='Count_Inter_cir')
Demand_Points_Overlapped_Catchment_Year_2=pd.merge(Demand_Points_Overlapped_Catchment_Year_2, Count_Inter_Cir_Year_2, on='Inter_cir')

# calculate catchment Area and excluding those having areas is zero.
Demand_Points_Overlapped_Catchment_Year_2['Catchment_Area']=(Demand_Points_Overlapped_Catchment_Year_2['radius']**2)*math.pi 
Demand_Points_Overlapped_Catchment_Year_2=Demand_Points_Overlapped_Catchment_Year_2[Demand_Points_Overlapped_Catchment_Year_2['Catchment_Area']>0]

# As the total overlapped points is less than Number of Health Post in the year so choosing from every records.
Overlapped_bins_Year_2=pds.read_csv_data_file(Dir_Year_2, Overlapped_bins_Name_Year_2)
Demand_Points_Overlapped_Catchment_Year_2=Extra.choose_rows(Demand_Points_Overlapped_Catchment_Year_2,Overlapped_bins_Year_2,Year_2)

# Converting DataFrame to GeoDataFrame
Demand_Points_Overlapped_Catchment_Year_2=gpd.GeoDataFrame(Demand_Points_Overlapped_Catchment_Year_2,geometry='geometry',crs=crs)
Demand_Points_OverlapCatch_Year_2=Demand_Points_Overlapped_Catchment_Year_2[['Inter_cir','geometry']]

# Calculating Catchment Pop.
Demand_Points_OverlapCatch_Year_2, Covered_Grid_Year_2=Varib.Catchment(Demand_Points_OverlapCatch_Year_2, Malawi_pop)

# Calculate the Catchment Population for all the years
Demand_Points_Overlapped_Catchment_Year_2=Extra.catchment_pop(Demand_Points_Overlapped_Catchment_Year_2,Demand_Points_OverlapCatch_Year_2, Year_2)

# From multiple points selecting according to CIP Number and taking first circle from overlapped.
Health_Post_Year_2=Demand_Points_Overlapped_Catchment_Year_2.sort_values(Year_2+'_pop_catch',ascending=False).head(Number_of_Health_Post_Year_2)
Health_Post_Year_2.drop(Year_2+'_pop_catch',axis=1,inplace=True)

# Convert the Health Post Datafrmae to Geopandas DataFrame and caluclates the catchment Population after that.
Health_Post_Year_2=gpd.GeoDataFrame(Health_Post_Year_2,geometry='geometry',crs=crs)
Health_Post_Overlapped_Year_2=Health_Post_Year_2[['Inter_cir','geometry']]
Health_Post_Overlapped_Year_2, Covered_Grid_Year_2 = Varib.Catchment(Health_Post_Overlapped_Year_2, Malawi_pop)

# Calculate the overall catchment Pop
Health_Post_Overlapped_Catchment_Year_2=Extra.catchment_pop(Health_Post_Year_2,Health_Post_Overlapped_Year_2, Year_2)

# Saving the Health Post Files
shp.write_shape_data_file(Dir_Year_2,Health_Post_Name_Year_2,Health_Post_Overlapped_Catchment_Year_2)
shp.write_shape_data_file(Dir_Year_2,Health_Post_Overlapped_Name_Year_2,Health_Post_Overlapped_Year_2)
shp.write_shape_data_file(Dir_Year_2,Covered_Grid_Name_Year_2,Covered_Grid_Year_2)

# Calulate overall covered Area

Covered_Areas_Year_2=Extra.Inc_Covered(Covered_Areas_Year_1, Covered_Grid_Year_2)

# Saving Covered Area File
shp.write_shape_data_file(Dir_Year_2,Covered_Areas_Name_Year_2,Covered_Areas_Year_2)
######################### Model 2 Year 2 Ends #########################################################





######################################## Model 2 Year 3 Starts ########################################
Dir_Year_3="F:/Model_2/Year_3/"


# Various Attributes for the Model 2
crs={'init': 'epsg:4326'}
Year_3='2022'
Pop_in_Clus_Year_3=12000
near_point_Year_3=5
clus_size_Year_3=100
Malawi_Map_Name_Year_3="Malawi_Distribution_"+Year_3
Uncover_TA_Name_Year_3="Uncovered_TA_"+Year_3
Overlapped_bins_Name_Year_3="Overlapped_bins_"+Year_3

Max_Radius_Year_3=6
Initial_Radius_Year_3=0.5
Increment_Radius_Year_3=0.5
Max_pop_in_circle_Year_3=12500
Number_of_Health_Post_Year_3=234
# Various Attributes for Likoma Island for Model 2
near_point_Likoma_Year_3=2
clus_size_Likoma_Year_3=1

# choosing variables for the Demand Points
Rank_Min_Year_3=220
Cluster_Min_Year_3=6000

# Model 2 Paths for the Output files
Malawi_cluster_Name_Year_3="Malawi_cluster_"+Year_3+'_'+str(near_point_Year_3)
Malawi_cluster_Likoma_Name_Year_3="Malawi_cluster_Likoma_"+Year_3+'_'+str(near_point_Likoma_Year_3)
Malawi_cluster_Merge_Name_Year_3="Malawi_cluster_Merge_"+Year_3
Probable_Points_Name_Year_3="Probable_Points"+Year_3
Demand_Points_Name_Year_3="Demand_Points_"+Year_3
Health_Post_Name_Year_3="Health_Posts_"+Year_3
Health_Post_Overlapped_Name_Year_3="Health_Posts_Overlapped_"+Year_3
Covered_Grid_Name_Year_3="Covered_Grid_Area_"+Year_3
Covered_Areas_Name_Year_3="Overall_Covered_Area_"+Year_3

# Applying Logistics

# Calculating the New Map of Malawi excluding the health post that covered in previous year.
Malawi_Map_Year_3=Extra.Map_Unc(Malawi_pop,Covered_Areas_Year_2)
Malawi_Map_Year_3=Malawi_Map_Year_3[['Country', 'District', 'TA_NAMES', 'TYPE_2', 'Type', 'ENGTYPE_2',
       'Grid_index', '2018_pop_1', '2019_pop_1', '2020_pop_1', '2021_pop_1', '2022_pop_1',
       '2023_pop_1', 'geometry']]

# Calculating uncovered population according to TA Wise.
Uncover_TA_Year_3=Extra.TA_Wise_Uncover(Malawi_Map_Year_3)

# Saving the Malawi Map and Uncover TA Files.
shp.write_shape_data_file(Dir_Year_3,Malawi_Map_Name_Year_3, Malawi_Map_Year_3)
pds.write_csv_data_file(Dir_Year_3,Uncover_TA_Name_Year_3,Uncover_TA_Year_3)

# Selecting Uncover Areas for the Malawi Map for Year 1
Malawi_pop=Malawi_Map_Year_3[Malawi_Map_Year_3['Type']=='Uncover'].copy()

# drop Null values from the Map
Malawi_pop.dropna(inplace=True)

# Removing National Park, Water body, Reserve Areas having Population 0
Malawi_pop_Year_3=Malawi_pop[~(Malawi_pop['TYPE_2'].isin(['National Park','Water body','Reserve']) & (Malawi_pop[Year_3+'_pop_1']==0))]

# Removing Likoma District because it is seperate island from the Map Malawi
Malawi_pop_Year_3=Malawi_pop[Malawi_pop['District']!='Likoma']

# Removing the very small areas for creating clusters easy and where the population for the Year 1 is zero.
Malawi_pop_Year_3['Area']=Malawi_pop_Year_3.geometry.area
Malawi_pop_Year_3=Malawi_pop_Year_3[(Malawi_pop_Year_3['Area']>1.724197566456432E-08) & (Malawi_pop_Year_3[Year_3+'_pop_1']!=0)]

# Creating the clusters by passing parameters through the Function
Malawi_cluster_Year_3=cluster.Cluster_creation(Malawi_pop_Year_3,near_point_Year_3,clus_size_Year_3,Year_3,Pop_in_Clus_Year_3)

# For Likoma Island creating the clusters
Malawi_pop_Likoma_Year_3=Malawi_pop[Malawi_pop['District']=='Likoma']

# Creating the clusters for Likoma islands by passing parameters through the Function
Malawi_cluster_Likoma_Year_3=cluster.Cluster_creation(Malawi_pop_Likoma_Year_3,near_point_Likoma_Year_3,clus_size_Likoma_Year_3,Year_3,Pop_in_Clus_Year_3)
Malawi_cluster_Likoma_Year_3=Malawi_cluster_Likoma_Year_3[:1]

# Merge the Likoma Islands file with Malawi_Map 
Malawi_cluster_Merge_Year_3=Mo_Ro.Merge_Row_DataFrame(Malawi_cluster_Year_3, Malawi_cluster_Likoma_Year_3)

# Probable Points for Year 1 based on Max Grid population in the cluster and then choose centroid.
Probable_Points_Year_3 = Prob.Prob_Points(Malawi_pop,Malawi_cluster_Merge_Year_3,Year_3,crs)

# Saving the files of Probable Points, Malawi_cluster_Merge, Malawi_cluster_likoma, Malawi_cluster.
shp.write_shape_data_file(Dir_Year_3,Malawi_cluster_Name_Year_3,Malawi_cluster_Year_3)
shp.write_shape_data_file(Dir_Year_3,Malawi_cluster_Likoma_Name_Year_3,Malawi_cluster_Likoma_Year_3)
shp.write_shape_data_file(Dir_Year_3,Malawi_cluster_Merge_Name_Year_3,Malawi_cluster_Merge_Year_3)
shp.write_shape_data_file(Dir_Year_3, Probable_Points_Name_Year_3,Probable_Points_Year_3)

## Swati works Here.
## Selecting Demand Points from Probable Points.

Demand_Points_Year_3=dp.demand_Points(Dis_Rank, Probable_Points_Year_3, GVH_ADM, Covered_Areas_Year_2, Rank_Min_Year_3, Cluster_Min_Year_3, Year_3, Disease_Flag)

## Saving Demand Points
shp.write_shape_data_file(Dir_Year_3,Demand_Points_Name_Year_3,Demand_Points_Year_3)
pds.write_csv_data_file(Dir_Year_3, Demand_Points_Name_Year_3, Demand_Points_Year_3)

## Sham Works here for Variable Radius Circle and Overlapping Circles.

# Variable Circle Formation
Demand_Points_Year_3=shp.read_shape_data_file(Dir_Year_3, Demand_Points_Name_Year_3)

Demand_Points_Catchment_Year_3=Varib.Variable_Radius(Demand_Points_Year_3, Malawi_pop, Max_Radius_Year_3,Initial_Radius_Year_3,Increment_Radius_Year_3,Max_pop_in_circle_Year_3,Year_3,crs)
Demand_Points_Catchment_Year_3=Demand_Points_Catchment_Year_3[['Country', 'District', 'TA_NAMES', 'cluster_id', 'GVH', 'D_TA', 'dvcl', 'radius', 'cvrd_poly', '2020_pop_1', '2021_pop_1', '2022_pop_1', '2023_pop_1']]

# Overlapping Circle Formation
Demand_Points_Catchment_Year_3.rename(columns={'cvrd_poly':'geometry'},inplace=True)
Demand_Points_Overlapped_Catchment_Year_3=Overlp.Overlapping_Areas(Demand_Points_Catchment_Year_3,crs={'init': 'epsg:4326'})

# Caluclating the count of Intersecting circles and merge it with Demand_Points_Overlapped_Catchment.
Count_Inter_Cir_Year_3=Demand_Points_Overlapped_Catchment_Year_3.groupby('Inter_cir',as_index=False).size().reset_index(name='Count_Inter_cir')
Demand_Points_Overlapped_Catchment_Year_3=pd.merge(Demand_Points_Overlapped_Catchment_Year_3, Count_Inter_Cir_Year_3, on='Inter_cir')

# calculate catchment Area and excluding those having areas is zero.
Demand_Points_Overlapped_Catchment_Year_3['Catchment_Area']=(Demand_Points_Overlapped_Catchment_Year_3['radius']**2)*math.pi 
Demand_Points_Overlapped_Catchment_Year_3=Demand_Points_Overlapped_Catchment_Year_3[Demand_Points_Overlapped_Catchment_Year_3['Catchment_Area']>0]

# As the total overlapped points is less than Number of Health Post in the year so choosing from every records.
Overlapped_bins_Year_3=pds.read_csv_data_file(Dir_Year_3, Overlapped_bins_Name_Year_3)
Demand_Points_Overlapped_Catchment_Year_3=Extra.choose_rows(Demand_Points_Overlapped_Catchment_Year_3,Overlapped_bins_Year_3,Year_3)

# Converting DataFrame to GeoDataFrame
Demand_Points_Overlapped_Catchment_Year_3=gpd.GeoDataFrame(Demand_Points_Overlapped_Catchment_Year_3,geometry='geometry',crs=crs)
Demand_Points_OverlapCatch_Year_3=Demand_Points_Overlapped_Catchment_Year_3[['Inter_cir','geometry']]

# Calculating Catchment Pop.
Demand_Points_OverlapCatch_Year_3, Covered_Grid_Year_3=Varib.Catchment(Demand_Points_OverlapCatch_Year_3, Malawi_pop)

# Calculate the Catchment Population for all the years
Demand_Points_Overlapped_Catchment_Year_3=Extra.catchment_pop(Demand_Points_Overlapped_Catchment_Year_3,Demand_Points_OverlapCatch_Year_3, Year_3)

# From multiple points selecting according to CIP Number and taking first circle from overlapped.
Health_Post_Year_3=Demand_Points_Overlapped_Catchment_Year_3.sort_values(Year_3+'_pop_catch',ascending=False).head(Number_of_Health_Post_Year_3)
Health_Post_Year_3.drop(Year_3+'_pop_catch',axis=1,inplace=True)

# Convert the Health Post Datafrmae to Geopandas DataFrame and caluclates the catchment Population after that.
Health_Post_Year_3=gpd.GeoDataFrame(Health_Post_Year_3,geometry='geometry',crs=crs)
Health_Post_Overlapped_Year_3=Health_Post_Year_3[['Inter_cir','geometry']]
Health_Post_Overlapped_Year_3, Covered_Grid_Year_3 = Varib.Catchment(Health_Post_Overlapped_Year_3, Malawi_pop)

# Calculate the overall catchment Pop
Health_Post_Overlapped_Catchment_Year_3=Extra.catchment_pop(Health_Post_Year_3,Health_Post_Overlapped_Year_3, Year_3)

# Saving the Health Post Files
shp.write_shape_data_file(Dir_Year_3,Health_Post_Name_Year_3,Health_Post_Overlapped_Catchment_Year_3)
shp.write_shape_data_file(Dir_Year_3,Health_Post_Overlapped_Name_Year_3,Health_Post_Overlapped_Year_3)
shp.write_shape_data_file(Dir_Year_3,Covered_Grid_Name_Year_3,Covered_Grid_Year_3)

# Calulate overall covered Area
Covered_Areas_Year_2=Covered_Areas_Year_2[['Area', 'Country', 'District', 'TA_NAMES', 'Type', 'geometry']]
Covered_Areas_Year_3=Extra.Inc_Covered(Covered_Areas_Year_2, Covered_Grid_Year_3)

# Saving Covered Area File
shp.write_shape_data_file(Dir_Year_3,Covered_Areas_Name_Year_3,Covered_Areas_Year_3)
######################### Model 2 Year 3 Ends #########################################################



######################## MOdel 2 Year 4 Starts ########################################################
Dir_Year_4="F:/Model_2/Year_4/"


# Various Attributes for the Model 2
crs={'init': 'epsg:4326'}
Year_4='2023'
Pop_in_Clus_Year_4=12000
near_point_Year_4=3
clus_size_Year_4=100
Malawi_Map_Name_Year_4="Malawi_Distribution_"+Year_4
Uncover_TA_Name_Year_4="Uncovered_TA_"+Year_4
Overlapped_bins_Name_Year_4="Overlapped_bins_"+Year_4

Max_Radius_Year_4=6
Initial_Radius_Year_4=0.5
Increment_Radius_Year_4=0.5
Max_pop_in_circle_Year_4=12500
Number_of_Health_Post_Year_4=234
# Various Attributes for Likoma Island for Model 2
near_point_Likoma_Year_4=2
clus_size_Likoma_Year_4=1

# choosing variables for the Demand Points
Rank_Min_Year_4=257
Cluster_Min_Year_4=0

# Model 2 Paths for the Output files
Malawi_cluster_Name_Year_4="Malawi_cluster_"+Year_4+'_'+str(near_point_Year_4)
Malawi_cluster_Likoma_Name_Year_4="Malawi_cluster_Likoma_"+Year_4+'_'+str(near_point_Likoma_Year_4)
Malawi_cluster_Merge_Name_Year_4="Malawi_cluster_Merge_"+Year_4
Probable_Points_Name_Year_4="Probable_Points"+Year_4
Demand_Points_Name_Year_4="Demand_Points_"+Year_4
Health_Post_Name_Year_4="Health_Posts_"+Year_4
Health_Post_Overlapped_Name_Year_4="Health_Posts_Overlapped_"+Year_4
Covered_Grid_Name_Year_4="Covered_Grid_Area_"+Year_4
Covered_Areas_Name_Year_4="Overall_Covered_Area_"+Year_4

# Applying Logistics

# Calculating the New Map of Malawi excluding the health post that covered in previous year.
Malawi_Map_Year_4=Extra.Map_Unc(Malawi_pop,Covered_Areas_Year_3)
Malawi_Map_Year_4=Malawi_Map_Year_4[['Country', 'District', 'TA_NAMES', 'TYPE_2', 'Type', 'ENGTYPE_2',
       'Grid_index', '2018_pop_1', '2019_pop_1', '2020_pop_1', '2021_pop_1', '2022_pop_1',
       '2023_pop_1', 'geometry']]

# Calculating uncovered population according to TA Wise.
Uncover_TA_Year_4=Extra.TA_Wise_Uncover(Malawi_Map_Year_4)

# Saving the Malawi Map and Uncover TA Files.
shp.write_shape_data_file(Dir_Year_4,Malawi_Map_Name_Year_4, Malawi_Map_Year_4)
pds.write_csv_data_file(Dir_Year_4,Uncover_TA_Name_Year_4,Uncover_TA_Year_4)

# Selecting Uncover Areas for the Malawi Map for Year 1
Malawi_pop=Malawi_Map_Year_4[Malawi_Map_Year_4['Type']=='Uncover'].copy()

# drop Null values from the Map
Malawi_pop.dropna(inplace=True)

# Removing National Park, Water body, Reserve Areas having Population 0
Malawi_pop_Year_4=Malawi_pop[~(Malawi_pop['TYPE_2'].isin(['National Park','Water body','Reserve']) & (Malawi_pop[Year_4+'_pop_1']==0))]

# Removing Likoma District because it is seperate island from the Map Malawi
Malawi_pop_Year_4=Malawi_pop[Malawi_pop['District']!='Likoma']

# Removing the very small areas for creating clusters easy and where the population for the Year 1 is zero.
Malawi_pop_Year_4['Area']=Malawi_pop_Year_4.geometry.area
Malawi_pop_Year_4=Malawi_pop_Year_4[(Malawi_pop_Year_4['Area']>1.724197566456432E-08) & (Malawi_pop_Year_4[Year_4+'_pop_1']!=0)]

# Creating the clusters by passing parameters through the Function
Malawi_cluster_Year_4=cluster.Cluster_creation(Malawi_pop_Year_4,near_point_Year_4,clus_size_Year_4,Year_4,Pop_in_Clus_Year_4)

# For Likoma Island creating the clusters
Malawi_pop_Likoma_Year_4=Malawi_pop[Malawi_pop['District']=='Likoma']

# Creating the clusters for Likoma islands by passing parameters through the Function
Malawi_cluster_Likoma_Year_4=cluster.Cluster_creation(Malawi_pop_Likoma_Year_4,near_point_Likoma_Year_4,clus_size_Likoma_Year_4,Year_4,Pop_in_Clus_Year_4)
Malawi_cluster_Likoma_Year_4=Malawi_cluster_Likoma_Year_4[:1]

# Merge the Likoma Islands file with Malawi_Map 
Malawi_cluster_Merge_Year_4=Mo_Ro.Merge_Row_DataFrame(Malawi_cluster_Year_4, Malawi_cluster_Likoma_Year_4)

# Probable Points for Year 1 based on Max Grid population in the cluster and then choose centroid.
Probable_Points_Year_4 = Prob.Prob_Points(Malawi_pop,Malawi_cluster_Merge_Year_4,Year_4,crs)

# Saving the files of Probable Points, Malawi_cluster_Merge, Malawi_cluster_likoma, Malawi_cluster.
shp.write_shape_data_file(Dir_Year_4,Malawi_cluster_Name_Year_4,Malawi_cluster_Year_4)
shp.write_shape_data_file(Dir_Year_4,Malawi_cluster_Likoma_Name_Year_4,Malawi_cluster_Likoma_Year_4)
shp.write_shape_data_file(Dir_Year_4,Malawi_cluster_Merge_Name_Year_4,Malawi_cluster_Merge_Year_4)
shp.write_shape_data_file(Dir_Year_4, Probable_Points_Name_Year_4,Probable_Points_Year_4)

## Swati works Here.
## Selecting Demand Points from Probable Points.

Demand_Points_Year_4=dp.demand_Points(Dis_Rank, Probable_Points_Year_4, GVH_ADM, Covered_Areas_Year_3, Rank_Min_Year_4, Cluster_Min_Year_4, Year_4, Disease_Flag)

## Saving Demand Points
shp.write_shape_data_file(Dir_Year_4,Demand_Points_Name_Year_4,Demand_Points_Year_4)
pds.write_csv_data_file(Dir_Year_4, Demand_Points_Name_Year_4, Demand_Points_Year_4)

## Sham Works here for Variable Radius Circle and Overlapping Circles.

# Variable Circle Formation
Demand_Points_Year_4=shp.read_shape_data_file(Dir_Year_4, Demand_Points_Name_Year_4)

Demand_Points_Catchment_Year_4=Varib.Variable_Radius(Demand_Points_Year_4, Malawi_pop, Max_Radius_Year_4,Initial_Radius_Year_4,Increment_Radius_Year_4,Max_pop_in_circle_Year_4,Year_4,crs)
Demand_Points_Catchment_Year_4=Demand_Points_Catchment_Year_4[['Country', 'District', 'TA_NAMES', 'cluster_id', 'GVH', 'D_TA', 'dvcl', 'radius', 'cvrd_poly', '2020_pop_1', '2021_pop_1', '2022_pop_1', '2023_pop_1']]

# Overlapping Circle Formation
Demand_Points_Catchment_Year_4.rename(columns={'cvrd_poly':'geometry'},inplace=True)
Demand_Points_Overlapped_Catchment_Year_4=Overlp.Overlapping_Areas(Demand_Points_Catchment_Year_4,crs={'init': 'epsg:4326'})

# Caluclating the count of Intersecting circles and merge it with Demand_Points_Overlapped_Catchment.
Count_Inter_Cir_Year_4=Demand_Points_Overlapped_Catchment_Year_4.groupby('Inter_cir',as_index=False).size().reset_index(name='Count_Inter_cir')
Demand_Points_Overlapped_Catchment_Year_4=pd.merge(Demand_Points_Overlapped_Catchment_Year_4, Count_Inter_Cir_Year_4, on='Inter_cir')

# calculate catchment Area and excluding those having areas is zero.
Demand_Points_Overlapped_Catchment_Year_4['Catchment_Area']=(Demand_Points_Overlapped_Catchment_Year_4['radius']**2)*math.pi 
Demand_Points_Overlapped_Catchment_Year_4=Demand_Points_Overlapped_Catchment_Year_4[Demand_Points_Overlapped_Catchment_Year_4['Catchment_Area']>0]

# Converting DataFrame to GeoDataFrame
Demand_Points_Overlapped_Catchment_Year_4=gpd.GeoDataFrame(Demand_Points_Overlapped_Catchment_Year_4,geometry='geometry',crs=crs)
Demand_Points_OverlapCatch_Year_4=Demand_Points_Overlapped_Catchment_Year_4[['Inter_cir','geometry']]

# Calculating Catchment Pop.
Demand_Points_OverlapCatch_Year_4, Covered_Grid_Year_4=Varib.Catchment(Demand_Points_OverlapCatch_Year_4, Malawi_pop)

# Calculate the Catchment Population for all the years
Demand_Points_Overlapped_Catchment_Year_4=Extra.catchment_pop(Demand_Points_Overlapped_Catchment_Year_4,Demand_Points_OverlapCatch_Year_4, Year_4)

# From multiple points selecting according to CIP Number and taking first circle from overlapped.
Health_Post_Year_4=Demand_Points_Overlapped_Catchment_Year_4.sort_values(Year_4+'_pop_catch',ascending=False).head(Number_of_Health_Post_Year_4)
Health_Post_Year_4.drop(Year_4+'_pop_catch',axis=1,inplace=True)

# Convert the Health Post Datafrmae to Geopandas DataFrame and caluclates the catchment Population after that.
Health_Post_Year_4=gpd.GeoDataFrame(Health_Post_Year_4,geometry='geometry',crs=crs)
Health_Post_Overlapped_Year_4=Health_Post_Year_4[['Inter_cir','geometry']]
Health_Post_Overlapped_Year_4, Covered_Grid_Year_4 = Varib.Catchment(Health_Post_Overlapped_Year_4, Malawi_pop)

# Calculate the overall catchment Pop
Health_Post_Overlapped_Catchment_Year_4=Extra.catchment_pop(Health_Post_Year_4,Health_Post_Overlapped_Year_4, Year_4)

# Saving the Health Post Files
shp.write_shape_data_file(Dir_Year_4,Health_Post_Name_Year_4,Health_Post_Overlapped_Catchment_Year_4)
shp.write_shape_data_file(Dir_Year_4,Health_Post_Overlapped_Name_Year_4,Health_Post_Overlapped_Year_4)
shp.write_shape_data_file(Dir_Year_4,Covered_Grid_Name_Year_4,Covered_Grid_Year_4)

# Calulate overall covered Area
Covered_Areas_Year_4=Extra.Inc_Covered(Covered_Areas_Year_3, Covered_Grid_Year_4)

# Saving Covered Area File
shp.write_shape_data_file(Dir_Year_4,Covered_Areas_Name_Year_4,Covered_Areas_Year_4)
################################ Model 2 Year 4 Ends ##########################################



############################## Evaluating RESULTS Starts ######################################
Dir_Year_5="F:/Model_2/END/"


# Various Attributes for the Model 2
crs={'init': 'epsg:4326'}
Year_5='END'
Malawi_Map_Name_Year_5="Malawi_Distribution_"+Year_5
Uncover_TA_Name_Year_5="Uncovered_TA_"+Year_5

# Applying Logistics

# Calculating the New Map of Malawi excluding the health post that covered in previous year.
Malawi_Map_Year_5=Extra.Map_Unc(Malawi_pop,Covered_Areas_Year_4)
Malawi_Map_Year_5=Malawi_Map_Year_5[['Country', 'District', 'TA_NAMES', 'TYPE_2', 'Type', 'ENGTYPE_2',
       'Grid_index', '2018_pop_1', '2019_pop_1', '2020_pop_1', '2021_pop_1', '2022_pop_1',
       '2023_pop_1', 'geometry']]

# Calculating uncovered population according to TA Wise.
Uncover_TA_Year_5=Extra.TA_Wise_Uncover(Malawi_Map_Year_5)

# Saving the Malawi Map and Uncover TA Files.
shp.write_shape_data_file(Dir_Year_5,Malawi_Map_Name_Year_5, Malawi_Map_Year_5)
pds.write_csv_data_file(Dir_Year_5,Uncover_TA_Name_Year_5,Uncover_TA_Year_5)
############################### Evaluation Results Ends ######################################### 