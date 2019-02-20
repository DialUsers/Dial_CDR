

############################################################################################
##  LONG TERM POPULATION MIGRATION MOVEMENT 
##
## 1. Estimate monthly population movement by netflows and proportion of inflow/outflow at TA level for each month.
##    Identify administrative units with floating population movements
##    Scale user density at tower over months to population density
##    Identify locations with consistent inflow and outflow of populations.
## 2. Identify new and inactive unique subscriber count at each tower. 
##    Calculate netflow change of population at each tower.
############################################################################################

####STEP 1 :############# IMPORT REQUIRED PACKAGES 
import pandas as pd 
import numpy as np 
import datetime 
import csv 
from datetime import datetime 
import pyodbc 
import os 
from collections import defaultdict, Counter 
from numpy.random import randn
from functools import reduce
import itertools  
from joblib import Parallel, delayed
import multiprocessing

#Read Data from HIVE
def readdatahive(connection,table_name):
    query="select * from etldb.{}".format(table_name)
    input_data=pd.read_sql_query(query,connection)
    input_data.columns=['call_originating_number','jan16','feb16','mar16','jun16','jul16','aug16','sep16',
       'oct16','nov16','dec16','jan17','feb17','mar17','apr17','may17','jun17','jul17','aug17','sep17',
       'oct17','nov17','dec17','jan18','feb18','apr18','may18']
    return input_data


#impute the null values with ffill
def ffillna(data):
    for i,row in data.iterrows():
        row.fillna(method='ffill', inplace=True)
    return data

#APPLY MULTIPROCESSING
def applyParallel(dfGrouped, func):
    retLst = Parallel(n_jobs=multiprocessing.cpu_count())(delayed(func)(group) for name, group in dfGrouped)
    return pd.concat(retLst)

#get inflow and outflow
def get_in_out(data,c,unique_TA):
    f=pd.DataFrame()
    for x in unique_TA:
        final=pd.DataFrame()
        i=0
        for j in range(len(c)):
            y=c[j]
            final.loc[i,'TA']=x
            #count=(data[c[j]]==x).sum()
            #count_x=(data[c[j]]!=x).sum()
            #Get the list of call_originating_numbers for a month for a TA
            sub=data.index[data[c[j]]==x].tolist()
            
            #Get all subscribers for a month in other TA's
            all_sub=data.index[data[c[j]]!=x].tolist()
            if j==25:
                pass
            else:
                z=c[j+1]
                #Get the list of call_originating_numbers for next month for a TA
                sub_next=data.index[data[c[j+1]]==x].tolist()
                
                #Get subscriber list which are there in next month for TA but not in earlier month 
                #for other TA
                s1=np.setdiff1d(sub_next,sub)
                s2=list(set(s1).intersection(all_sub))
                final.loc[i,'Outflow_{}'.format(y)]=len(np.setdiff1d(sub,sub_next))
                final.loc[i,'inflow_{}'.format(z)]=len(s2)
        
        f=f.append(final) 
        i=i+1
        
    return f

#get netflow 
def get_netflow_cnt(data,c):
    netflow=pd.DataFrame()
    for i,row in data.iterrows():
        for j in range(len(c)):
            x=c[j]
            if j==25:
                pass
            else:
                y=c[j+1]
                netflow.loc[i,'netflow_{}'.format(y)]=data.loc[i,y]-data.loc[i,x]
    
    return netflow

 

if __name__ == "__main__":
    
    pyodbc.autocommit = True 
    con = pyodbc.connect("DSN=malawihiveodbc", autocommit=True) 
    cursor = con.cursor()
    
    #tablename
    table="user_homelocation_monthly_ta"
    
    #input data reading using above 
    long_term_input=readdatahive(con,table)
    
    long_term_input.set_index('call_originating_number',inplace=True)
    
    long_term_input=applyParallel(long_term_input.groupby(long_term_input.index), ffillna)
    
    b=[]
    for x in long_term_input.columns:
        a=long_term_input[x].unique().tolist()
        b.append(a)
    
    #Get the unique TA's across 26 months 
    unique_TA=list(reduce(set.intersection, [set(item) for item in b ]))
    c=list(long_term_input.columns)
    
    in_out_flow=get_in_out(long_term_input,c,unique_TA)
    
    in_out_flow.set_index('TA',inplace=True)
    filter_outflow = [col for col in in_out_flow if col.startswith('Outflow')]
    outflow=b[filter_outflow]      

    filter_inflow = [col for col in in_out_flow if col.startswith('inflow')]  
    inflow=b[filter_inflow]
    
    #Get the counts for each TA
    count_each_TA = long_term_input.apply(pd.value_counts)
    count_each_TA.reset_index(inplace=True)
    count_each_TA = count_each_TA.rename(columns={'index': 'Name_TA'})
    
    netflow_count=get_netflow_cnt(count_each_TA,c)
    


