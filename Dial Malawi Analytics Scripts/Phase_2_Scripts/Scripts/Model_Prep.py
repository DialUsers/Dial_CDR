# -*- coding: utf-8 -*-
"""
Created on Mon Feb  4 14:54:08 2019
"""
#################################################################
## The code for making grids on the map & raster with world pop##
## 2015 and discover the uncovered and covered areas of Malawi.##
#################################################################

# IMPORT PACKAGES
import numpy as np
import gdal
import rasterstats
from rasterstats import zonal_stats
import os
from pprint import pprint
import pandas as pd
import geopandas as gpd
import fiona
from fiona.crs import from_epsg
from pyproj import Proj
from shapely.geometry import Polygon, mapping

## Function to build & write the grid file.
def grid_shape(xmin,ymin,xmax,ymax,gridWidth,gridHeight,rows,cols,grid_shp_file):    
    ringXleftOrigin = xmin
    ringXrightOrigin = xmin + gridWidth
    ringYtopOrigin = ymax
    ringYbottomOrigin = ymax-gridHeight
    schema = {'geometry': 'Polygon','properties': {'test': 'int'}}
    with fiona.open(grid_shp_file,'w','ESRI Shapefile', schema ,crs=from_epsg(4326)) as c:
        for i in np.arange(rows):
            ringXleft = ringXleftOrigin
            ringXright =ringXrightOrigin
            for j in np.arange(cols):
                polygon = Polygon([(ringXleft, ringYtopOrigin), (ringXright, ringYtopOrigin), (ringXright, ringYbottomOrigin), (ringXleft, ringYbottomOrigin)])
                c.write({'geometry':mapping(polygon),'properties':{'test':1}})
                ringXleft = ringXleft + gridWidth
                ringXright = ringXright + gridWidth
            ringYtopOrigin = ringYtopOrigin - gridHeight
            ringYbottomOrigin = ringYbottomOrigin - gridHeight


def read_shape_data_file(filename):
    #location = dir_shp_read + filename + '.shp'
    data=gpd.read_file(filename)
    return data

def write_shape_data_file(filename, df_write):
    #location = dir_shp_write + filename + '.shp'
    df_write.to_file(filename)
    return

def read_data_xlsx(filename,sheetname):
    data=pd.read_excel(filename,sheetname=sheetname,header=0)    
    return data

def write_data_xlsx(filename,df):
    df.to_excel(filename)
    return

## Path for country Malawi Shape file
Malawi_shape_path="D:/dial_codes/Inputs/MWI_adm/MWI_adm2.shp"

## Grid File Path
grid_shp_file='grid.shp'

## Path for writing Malawi grid file
Malawi_grid_shp_file="Malawi_grid_shp.shp"

## Read the covered area Path
coveredarea_shape_Path="D:/dial_codes/Inputs/SA/SA_BestCase.shp"

## Path for writing the uncovered & cvered areas
uncoveredgrid_shp_file="uncovered.shp"
coveredgrid_shp_file="covered.shp"

## Raster file path
raster_tiff_file="D:/dial_codes/Inputs/MWI_ADM_WP/MWI_ppp_2015_adj_v2.tif"

## write the file of stats of raster file with Malawi
ppp_2015_wppp_uncover_path="district_population_wppp_uncover.xlsx"
ppp_2015_wppp_cover_path="district_population_wppp_cover.xlsx"

## Pop adjusted data path with sheetname
Pop_adj_path="D:/dial_codes/Inputs/Population_Forecast_adjusted_Version_1.23.xlsx"
sheetname="Pop_Estimate_Adj_NF_Worldpop"

## Malawi cover & uncover shape file
Malawi_Distribution_Path='Malawi_Distribution.shp'
Malawi_Distribution_CSV_Path="Distribution_Malawi.csv" 
## Calculating bounds (Max, Min in both direction Horizontal, Vertical) of the Malawi Map
xmin,ymin,xmax,ymax = fiona.open(Malawi_shape_path).bounds

## Now calculate the width or per square km area by using convention 1Degree->111Km.
##Caluclating according to 1.5 Km area in x and y direction so 1.5/111=0.0135
gridWidth = 0.00637   
gridHeight = 0.00637

## calculating how many grid cells accrding to vertical & Horizontal
rows = (ymax-ymin)/gridHeight
cols = (xmax-xmin)/gridWidth


## write grid shape file
grid_shape(xmin,ymin,xmax,ymax,gridWidth,gridHeight,rows,cols,grid_shp_file)
        
## Overlay the grid with country map
            
## Read the Shape files of grid & Malawi Shape files 
Malawi_shp=read_shape_data_file(Malawi_shape_path)
grid_shp = read_shape_data_file(grid_shp_file)

## Reset the index to find out Grid Index
grid_shp.reset_index(level=0,inplace=True)
grid_shp=grid_shp[['index','geometry']]
grid_shp.rename(columns={'index':'Grid_index'},inplace=True)

## Overlay Malawi with grid file 
Malawi_grid_shp = gpd.overlay(Malawi_shp,grid_shp,how='intersection')
write_shape_data_file(Malawi_grid_shp_file, Malawi_grid_shp)

## Read the shape files of covered area shape file
coveredarea_shape = read_shape_data_file(coveredarea_shape_Path)

## Changing crs to 4326 crs
coveredarea_shape=coveredarea_shape.to_crs({'init': 'epsg:4326'})

## Find out the areas of Uncovered & Covered areas of Population
areas_uncovered=gpd.overlay(Malawi_grid_shp, coveredarea_shape, how='difference')
areas_covered=gpd.overlay(Malawi_grid_shp, coveredarea_shape, how='intersection')

write_shape_data_file(uncoveredgrid_shp_file,areas_uncovered)
write_shape_data_file(coveredgrid_shp_file,areas_covered)

stats = zonal_stats(uncoveredgrid_shp_file,raster_tiff_file, stats="count sum min mean max median")
Pop_2015_uncover=pd.DataFrame(stats)
write_data_xlsx(ppp_2015_wppp_uncover_path,Pop_2015_uncover)
areas_uncovered.reset_index(drop=True, inplace=True)
Merge_Grids_uncover=pd.concat([Pop_2015_uncover,areas_uncovered], axis=1)
Merge_Grids_uncover['Type']='Uncover'

stats = zonal_stats(coveredgrid_shp_file,raster_tiff_file, stats="count sum min mean max median")
Pop_2015_cover=pd.DataFrame(stats)
write_data_xlsx(ppp_2015_wppp_cover_path,Pop_2015_cover)
areas_covered=areas_covered[['ID_0', 'ISO', 'NAME_0', 'ID_1', 'NAME_1', 'ID_2', 'NAME_2', 'TYPE_2', 'ENGTYPE_2', 'NL_NAME_2', 'VARNAME_2', 'Grid_index', 'geometry']]
areas_covered.reset_index(drop=True, inplace=True)
Merge_Grids_cover=pd.concat([Pop_2015_cover,areas_covered], axis=1)
Merge_Grids_cover['Type']='Cover'

Malawi_2015_dist = pd.concat([Merge_Grids_uncover,Merge_Grids_cover],axis=0,ignore_index=True)

SUM_2015_Pop_dist_TA=Malawi_2015_dist.groupby(['NAME_0','NAME_1','NAME_2'],as_index=False)[['sum']].sum()

Pop_adj_Data=read_data_xlsx(Pop_adj_path,sheetname)
Pop_adj_Data=Pop_adj_Data[['District','TA','2018_pop_adj', '2019_pop_adj', '2020_pop_adj', '2021_pop_adj', '2022_pop_adj', '2023_pop_adj']]
Pop_adj_Data['District']=Pop_adj_Data['District'].str.title()
SUM_2015_Pop_dist_TA=SUM_2015_Pop_dist_TA.merge(Pop_adj_Data,left_on=['NAME_1','NAME_2'],right_on=['District','TA'],how='left')
SUM_2015_Pop_dist_TA.rename(columns={'sum':'MAX_SUM'},inplace=True)

Forecast_Malawi_Pred=Malawi_2015_dist.merge(SUM_2015_Pop_dist_TA,on=['NAME_0','NAME_1','NAME_2'])

Forecast_Malawi_Pred['Percent_Grid_TA']=(Forecast_Malawi_Pred['sum']/Forecast_Malawi_Pred['MAX_SUM'])*100
Forecast_Malawi_Pred.fillna({'sum':0,'Percent_Grid_TA':0},inplace=True);


for column in Forecast_Malawi_Pred.columns:
    if column.endswith('adj'):
        Forecast_Malawi_Pred[column+'sumpp']=(Forecast_Malawi_Pred['Percent_Grid_TA']*Forecast_Malawi_Pred[column])/100

Forecast_selected_cols=Forecast_Malawi_Pred[['NAME_0', 'NAME_1', 'NAME_2', 'TYPE_2', 'Type', 'ENGTYPE_2', 'Grid_index', 'geometry', 'sum', 'MAX_SUM','Percent_Grid_TA',
       '2018_pop_adj', '2019_pop_adj', '2020_pop_adj', '2021_pop_adj',
       '2022_pop_adj', '2023_pop_adj', '2018_pop_adjsumpp',
       '2019_pop_adjsumpp', '2020_pop_adjsumpp', '2021_pop_adjsumpp',
       '2022_pop_adjsumpp', '2023_pop_adjsumpp',]].copy()

Forecast_selected_cols.rename(columns={'NAME_0':'Country','NAME_1':'District','NAME_2':'TA_NAMES','sum':'2015_wordpop_adjsumpp','MAX_SUM':'2015_wordpop_adj','Percent_Grid_TA':'Pop_Percent_Grid'},inplace=True)
#write_data_xlsx(Malawi_Distribution_CSV_Path,Forecast_selected_cols)
Forecast_selected_cols.to_csv(Malawi_Distribution_CSV_Path)
Forecast_geo_frame=gpd.GeoDataFrame(Forecast_selected_cols,geometry='geometry')
Forecast_geo_frame.crs=({'init': 'epsg:4326'})
write_shape_data_file(Malawi_Distribution_Path,Forecast_geo_frame)
