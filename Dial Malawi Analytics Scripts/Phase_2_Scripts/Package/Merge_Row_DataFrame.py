# -*- coding: utf-8 -*-
"""
Created on Thu Mar 14 15:34:47 2019
This is for merging the clusters from multiple islands.
"""

import pandas as pd

def Merge_Row_DataFrame(DataFrame1, DataFrame2):
    Malawi_DataFrame=pd.concat([DataFrame1,DataFrame2],axis=0)
    return Malawi_DataFrame