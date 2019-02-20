# -*- coding: utf-8 -*-
import pysal
import numpy as np
import pandas as pd
import geopandas as gpd
from pysal.spreg import ols
from pysal.spreg import ml_error
from pysal.spreg import ml_lag   
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import LeaveOneOut 


#read shapefile
def read_shape_data_file(dir_shp_read, filename):
    location = dir_shp_read + filename + '.shp'
    data=gpd.read_file(location)
    return data

#Write shapefile
def write_shape_data_file(dir_shp_write, filename, df_write):
    location = dir_shp_write + filename + '.shp'
    df_write.to_file(location)
    return

def get_ols_check_spatial_correlation(central_shape,location_shp_file,year):
    
    #Get the log transform of dependent and independent variables
    X_log = np.log(central_shape['Call_density'])
    Y_log = np.log(central_shape['{}_pop_density'.format(year)])
    
    X_log = np.array(X_log).T
    Y_log = np.array(Y_log)
    Y_log.shape = (len(Y_log),1)
    X_log.shape = (len(X_log),1)
    ###fit ols model for log transformed variable##################################
    ls = ols.OLS(Y_log, X_log) 
    central_ols=ls.summary
    
    ###Distance based weight matrix#############################################
    thresh = pysal.min_threshold_dist_from_shapefile(location_shp_file)
    wt = pysal.weights.DistanceBand.from_shapefile(location_shp_file, threshold=thresh, binary=True) 
    mi_distance = pysal.Moran(ls.u, wt, two_tailed=False)
    
    #Get Moran's I P-value using distance based weight matrix
    a=mi_distance.p_norm
    
    if mi_distance.p_norm<0.05:
        #If p_value less than 0.05 then we are going ahead with lagrange's test
        #To check whether to go with lag model or error model
        #but in this case we are going with error model
        lms=pysal.spreg.diagnostics_sp.LMtests(ls,wt)
        
        if lms.lme[1]<0.05:
            spat_err = ml_error.ML_Error(Y_log, X_log, wt)
            central_spat_err_distance=spat_err.summary
              
    return central_ols, a,central_spat_err_distance


#Random K-fold
def random_Kfold_results(central_shape,year):
    #Get the log transform of dependent and independent variables
    X = pd.DataFrame(np.log(central_shape['Call_density'])).reset_index(drop=True)
    y = pd.DataFrame(np.log(central_shape['{}_pop_density'.format(year)])).reset_index(drop=True)
    #leave one out 
    loo = LeaveOneOut()
    loo.get_n_splits(X)
    
    coef=pd.DataFrame()
    
    for train_index, test_index in loo.split(X):
       print("TRAIN:", train_index, "TEST:", test_index)
       #Get Train and Test dataset
       X_train, X_test = X.loc[train_index], X.loc[test_index]
       y_train, y_test = y.loc[train_index], y.loc[test_index]
       
       #fit the model linear reg
       lm = LinearRegression()
       lm.fit(X_train,y_train)
       
       #RMSE train and test
       RMSEtrain=np.sqrt(mean_squared_error(y_train,lm.predict(X_train)))
       RMSEtest=np.sqrt(mean_squared_error(y_test,lm.predict(X_test)))
       
       #Get the coefficient for all iterations
       coef=coef.append({'Alpha':float(lm.intercept_),'Beta':float(lm.coef_),'R^2':lm.score(X_train,y_train),
                         'RMSE_train':RMSEtrain,'RMSE_test':RMSEtest},ignore_index=True)

    return coef


#spatial K-fold   
def spatial_kfold(central_shape,year,thresh):
    
    data=central_shape[['Call_density','geometry', '{}_pop_density'.format(year)]].reset_index(drop=True)
    #Get the centoid for each TA
    data['centroid']=data['geometry'].centroid
    loo = LeaveOneOut()
    loo.get_n_splits(data)
    
    coef_sp=pd.DataFrame()

    for train_index, test_index in loo.split(data):
       print("TRAIN:", train_index, "TEST:", test_index)
       train=data.loc[train_index].reset_index(drop=True)
       test=data.loc[test_index].reset_index(drop=True)
       train_new=pd.DataFrame()
       
       #check whether training points and test points distance between threshold
       #is greater than threshold
       for i,row in train.iterrows():
           if test.centroid.distance(train.centroid.iloc[i]).values> thresh:
               train_new=train_new.append(train.iloc[i])
       
       #Get the train and test datasets
       X_train = pd.DataFrame(np.log(train_new['Call_density'])).reset_index(drop=True)
       y_train = pd.DataFrame(np.log(train_new['{}_pop_density'.format(year)])).reset_index(drop=True)
       X_test = pd.DataFrame(np.log(test['Call_density'])).reset_index(drop=True)
       y_test = pd.DataFrame(np.log(test['{}_pop_density'.format(year)])).reset_index(drop=True)
       
       #Fit regression model
       lm = LinearRegression()
       lm.fit(X_train,y_train)
       
       #Get RMSE train and test
       RMSEtrain=np.sqrt(mean_squared_error(y_train,lm.predict(X_train)))
       RMSEtest=np.sqrt(mean_squared_error(y_test,lm.predict(X_test)))
       
       #Get the coefficient for all iterations
       coef_sp=coef_sp.append({'Alpha':float(lm.intercept_),'Beta':float(lm.coef_),'R^2':lm.score(X_train,y_train),
                         'RMSE_train':RMSEtrain,'RMSE_test':RMSEtest},ignore_index=True)
    
    return coef_sp


if __name__ == "__main__":
    
    # Directory where embedding files are stored
    dir_shp_read="D:/Dial_codes/call_pop_density_shapefile/"
    dir_shp_write="D:/Dial_codes/"
    
    #filenames which are required
    year="2016"
    shp_pcd_year="malawi_pcd_TA_{}".format(year)
    shp_central_pcd_year="central_malawi_{}_new".format(year)
    central_shp = dir_shp_write + shp_central_pcd_year +'.shp'
    
    #configuration
    thresh = pysal.min_threshold_dist_from_shapefile(central_shp)

    #Read and write the inputs
    malawi_pcd=read_shape_data_file(dir_shp_read,shp_pcd_year)
    malawi_pcd.columns=['District', 'TA', 'Zone', 'Sum (ppp)', '{}_pop'.format(year), 'Call_density','Total_TA_area', 'geometry']
    malawi_pcd['{}_pop_density'.format(year)]=malawi_pcd['{}_pop'.format(year)]/malawi_pcd['Total_TA_area']
    malawi_pcd['Call_density']=malawi_pcd['Call_density']/malawi_pcd['Total_TA_area']
    malawi_pcd_central=malawi_pcd[malawi_pcd['Zone']=='Central']
    write_shape_data_file(dir_shp_write,shp_central_pcd_year,malawi_pcd_central)
    
    central_ols_results,p_value,spatial_error_model=get_ols_check_spatial_correlation(malawi_pcd_central,central_shp,year)
    
    random_kfold_coef=random_Kfold_results(malawi_pcd_central,year)
    
    randm_spkfold_coef=spatial_kfold(malawi_pcd_central,year,thresh)


