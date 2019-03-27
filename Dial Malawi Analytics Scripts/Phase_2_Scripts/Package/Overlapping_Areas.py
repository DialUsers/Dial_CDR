# -*- coding: utf-8 -*-
"""
Created on Fri Mar 15 12:30:24 2019
This Module calculate the Indexes of overlapping circles.
"""

import geopandas as gpd
import pandas as pd
from shapely.geometry import shape
from rtree import index

def Overlapping_Areas(facilities,crs={'init': 'epsg:4326'}):
    
    facilities.crs=crs
    circles=facilities['geometry'].tolist()
    cir_inter={}
    for posl,circle in enumerate(circles):
        values=[]
        for pos,circ in enumerate(circles):
            if posl != pos and shape(circle).intersects(circ):
                values.append(pos) 
        cir_inter[posl]=values    
    
    for key in cir_inter.keys():
        values=cir_inter.get(key)
        if not values:
            facilities.at[key,'Inter_cir']=str(key)
            continue
        values.append(key)
        values.sort()
        values='_'.join(map(str,values))
        facilities.at[key,'Inter_cir']= values
                
    for index in facilities.index:
        values=facilities.at[index,'Inter_cir']
        if str(index)!=values:
            indexes=values.split('_')
            indexes=list(map(int,indexes))
            i=0
            while i<len(indexes):
                if facilities.at[indexes[i],'Inter_cir']!=values:
                    new_indexes=facilities.at[indexes[i],'Inter_cir']
                    new_indexes=new_indexes.split('_')
                    new_indexes=list(map(int,new_indexes))
                    indexes=list(set(new_indexes+indexes))
                    indexes.sort()
                    values='_'.join(map(str,indexes))
                    facilities.at[indexes[i],'Inter_cir']=values
                    i=0
                i+=1
        continue
    return facilities
