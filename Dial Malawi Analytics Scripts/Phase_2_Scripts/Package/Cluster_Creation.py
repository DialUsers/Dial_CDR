# -*- coding: utf-8 -*-
"""
Created on Thu Mar 14 12:11:08 2019
This Module is useed for creating the clusters on the Grid.
"""

import numpy as np
import pandas as pd
import geopandas as gpd
from scipy.spatial import cKDTree
import itertools


def nearest(df,geom_union,nearest_point,Year,Pop_in_Clus,Flag=False):
    btree = cKDTree(geom_union)
    dist, idx = btree.query(geom_union,k=nearest_point)
    #dist=np.delete(dist,(0),axis=1).ravel()
    idx=np.delete(idx,(np.arange(nearest_point-1)),axis=1).ravel()
    df1 = pd.DataFrame.from_dict({'nearest_id' : df.loc[idx, 'cluster_id'].values, "{}{}".format(Year,'_pop_2'): df.loc[idx, Year+'_pop_1'].values })
    df=pd.concat([df,df1],axis=1)
    df['Merge_pop']=df[Year+'_pop_1']+df[Year+'_pop_2']
    df['clus_near']=df.apply(addcolumn,Year=Year,Pop_in_Clus=Pop_in_Clus,axis=1)
    df1=df.groupby('clus_near').size().reset_index(name='count')
    df=pd.merge(df,df1,on='clus_near')
    if Flag==True:
        df=brute_near(df,Year,Pop_in_Clus)
    #df['nearest_id']=np.where(df['Merge_pop']<=10000,df['nearest_id'],'NOT')
    df['nearest_id']=df.apply(lambda row:row['nearest_id'] if (row['Merge_pop']<=Pop_in_Clus and row['count']==2) else 'NOT',axis=1)
    df.drop([Year+'_pop_2','Merge_pop','count','clus_near'],axis=1,inplace=True)
    return df


def brute_near(df,Year,Pop_in_Clus):
    serviced_cluster=[]
    for index in df.index:
        if df.at[index,'count']==1 and df.at[index,'Merge_pop']<=Pop_in_Clus and df.at[index,'cluster_id'] not in serviced_cluster:
            nearest_point=df.at[index,'nearest_id']
            pop_2=df.at[index, Year+'_pop_2']
            merge_pop=df.at[index,'Merge_pop']
            clus_near=df.at[index,'clus_near']
            serviced_cluster.append(df.at[index,'cluster_id'])
            df.at[df['cluster_id']==nearest_point, Year+'_pop_2']=pop_2
            df.at[df['cluster_id']==nearest_point,'Merge_pop']=merge_pop
            df.at[df['cluster_id']==nearest_point,'clus_near']=clus_near
            df.at[df['cluster_id']==nearest_point,'nearest_id']=df.at[index,'cluster_id']
            serviced_cluster.append(nearest_point)
    df.drop('count',axis=1,inplace=True)
    df1=df.groupby('clus_near').size().reset_index(name='count')
    df1=pd.merge(df,df1,on='clus_near')
    return df1


def addcolumn(row,Year,Pop_in_Clus):
    if row[Year+'_pop_1']<=Pop_in_Clus and row['nearest_id']!='NOT':
        ls=[str(row['cluster_id']),str(row['nearest_id'])]
        ls=list(set(itertools.chain.from_iterable(el.split('_') for el in ls)))
        ls=list(map(int,ls))
        ls.sort()
        ls='_'.join(map(str, ls))
        return ls
    return(str(row['cluster_id']))

def Cluster_creation(Malawi_pop,near_point,clus_size,Year, Pop_in_Clus):
    Malawi_pop.sort_values('Grid_index',inplace=True)
    Malawi_pop.reset_index(drop=True,inplace=True)
    Malawi_pop.reset_index(level=0,inplace=True)
    Malawi_pop.rename(columns={'index':'cluster_id'},inplace=True)
    Malawi_prep=Malawi_pop[['cluster_id','2020_pop_1', '2021_pop_1', '2022_pop_1', '2023_pop_1', 'geometry']]
    nearest_point=2
    Flag=False
    while(Malawi_prep.shape[0]>clus_size and nearest_point<=near_point):
        Malawi_prep['Centroid']=Malawi_prep.geometry.centroid
        geom_union=list(zip(Malawi_prep.set_geometry('Centroid').geometry.x,Malawi_prep.set_geometry('Centroid').geometry.y))
        Malawi_chk=nearest(Malawi_prep,geom_union,nearest_point,Year,Pop_in_Clus,Flag)
        Malawi_chk['cluster_id']=Malawi_chk.apply(addcolumn,Year=Year,Pop_in_Clus=Pop_in_Clus,axis=1)
        Malawi_chk=Malawi_chk[['geometry', '2020_pop_1', '2021_pop_1', '2022_pop_1','2023_pop_1', 'cluster_id']]    
        #Malawi_chk['geometry']=Malawi_chk.buffer(0.01)
        Malawi_chk=Malawi_chk.dissolve(by='cluster_id',aggfunc='sum',as_index=False)    
        if Malawi_prep.shape[0]==Malawi_chk.shape[0] and Flag==False:
            #nearest_point+=1
            Flag=True
            continue
        elif Malawi_prep.shape[0]==Malawi_chk.shape[0] and Flag==True:
            nearest_point+=1
            Flag=False
            continue
        nearest_point=2
        Malawi_prep=Malawi_chk
        Flag=False
    return Malawi_chk
