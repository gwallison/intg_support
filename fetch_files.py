# -*- coding: utf-8 -*-
"""
Created on Fri Apr 21 14:31:11 2023

@author: garya

This set of routines is used as a standard way to interact with files on the 
online repositories.  

Dataframes will typically be stored and read as parquet.

"""
import pandas as pd
import os
from intg_support.common import completed
# from intg_support.file_handlers import get_df

def fetch_repo_full_df(outdir='./tmp',repo='current_repo',
                       repo_root='https://storage.googleapis.com/open-ff-common/repos'):
    
    import urllib.request
    fn = 'full_df.parquet'
    df_url = repo_root+'/'+repo+'/'+fn
    loc_fn = os.path.join(outdir,fn)
    print('Downloading full Open-FF data set.  Please wait...')
    try:
        urllib.request.urlretrieve(df_url,loc_fn)
    except:
        completed(False,'Problem downloading parquet file from repository!')
        return


    # try:
    #     df = pd.read_parquet(loc_fn)
    #     completed()
    #     return df
    # except:
    #     completed(False,'Problem making dataframe from file')

# def store_df_as_csv(df,fn,encoding='utf-8'):
#     t = df.copy()
#     for col in lst_str_cols:
#         if col in t.columns:
#             # print(col)
#             t[col] = "'"+t[col]
#     t.to_csv(fn,encoding=encoding)
    
# def get_csv(fn,check_zero=True,encoding='utf-8',sep=',',quotechar='"',
#             str_cols = lst_str_cols):
#     # check_zero: make sure str fields don't have "'" in zero position
#     dict_dtypes = {x : 'str'  for x in str_cols}
#     t = pd.read_csv(fn,encoding=encoding, low_memory=False, sep=sep,
#                     quotechar=quotechar, dtype=dict_dtypes)
#     if check_zero:
#         for col in str_cols:
#             if col in t.columns:
#                 #print(col)
#                 test = t[col].str[0]== "'"
#                 assert test.sum()==0, f'Initial single quote detected in col: {col}\nUsually means file destined for spreadsheet, not pandas.'
#     return t

# def save_df(df,fn):
#     if fn[-4:]=='.csv':
#         store_df_as_csv(df,fn)
#     else:
#         df.to_parquet(fn)
    
# def get_df(fn,cols=None):
#     if fn[-4:]=='.csv':
#         return get_csv(fn)
#     return pd.read_parquet(fn,columns=cols)




# if __name__ == '__main__':
#     get_csv(r"C:\MyDocs\OpenFF\src\testing\tmp\chartest.csv")
