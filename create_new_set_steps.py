# -*- coding: utf-8 -*-
"""
Created on Fri May  5 18:26:56 2023

@author: garya

Used to guide the colab-type builder code.  Each routine runs some steps for a 
cell.
"""

# Preamble: run as soon as this module is imported

import os, shutil
import pandas as pd
from IPython.display import HTML, display
from IPython.display import Markdown as md
import requests
import datetime
import urllib.request
from intg_support.file_handlers import store_df_as_csv, get_csv, save_df, get_df
import intg_support.fetch_new_bulk_data as fnbd
import intg_support.Bulk_data_reader as bdr

use_itables = True

root_dir = ''
orig_dir = os.path.join(root_dir,'orig_dir')
work_dir = os.path.join(root_dir,'work_dir')
final_dir = os.path.join(root_dir,'final')
ext_dir = os.path.join(root_dir,'ext')


if use_itables:
    from itables import init_notebook_mode
    init_notebook_mode(all_interactive=True)
    from itables import show as iShow
    import itables.options as opt
    opt.classes="display compact cell-border"
    opt.maxBytes = 0
    opt.maxColumns = 0
else:
    def iShow(df,maxBytes=0,classes=None):
        display(df)
       
        
def clr_cell(txt='Cell Completed', color = '#cfc'):
    t = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    s = f"""<div style="background-color: {color}; padding: 10px; border: 1px solid green;">"""
    s+= f'<h3> {txt} </h3> {t}'
    s+= "</div>"
    display(md(s))

def completed(status=True,txt=''):
    if txt =='':
        if status:
            txt = 'This step completed normally.'
        else:
            txt ='Problems encountered in this cell! Resolve before continuing.' 
    if status:
        clr_cell(txt)
    else:
        clr_cell(txt,color='pink')

def get_raw_df(cols=None):
  """without a list of cols, whole df will be returned"""
  return pd.read_parquet(os.path.join(work_dir,'raw_flat.parquet'),
                         columns=cols)
def create_and_fill_folders(download_repo=True,unpack_to_orig=True):
    dirs = [orig_dir,work_dir,final_dir,ext_dir]
    for d in dirs:
        if os.path.isdir(d):
            print(f'Directory exists: {d}')
        else:
            print(f'Creating directory: {d}')
            os.mkdir(d)
        if d==final_dir:
            others = ['pickles','curation_files','CAS_ref_files','CompTox_ref_files']
            for oth in others:   
                subdir = os.path.join(d,oth)
                if os.path.isdir(os.path.join(subdir)):
                    print(f'Directory exists: {subdir}')
                else:
                    print(f'Creating directory: {subdir}')
                    os.mkdir(subdir)
        if d==work_dir:
            others = ['new_CAS_REF','new_COMPTOX_REF']
            for oth in others:   
                subdir = os.path.join(d,oth)
                if os.path.isdir(os.path.join(subdir)):
                    print(f'Directory exists: {subdir}')
                else:
                    print(f'Creating directory: {subdir}')
                    os.mkdir(subdir)
    
    s_repo_name = os.path.join(orig_dir,'cloud_repo.zip')
    
    if download_repo:
        url = 'https://storage.googleapis.com/open-ff-common/repos/cloud_repo.zip'
        print(url)
        try:
          urllib.request.urlretrieve(url, s_repo_name)
        except:
          completed(False,'Problem downloading repository!')
        print('Continuing without downloading fresh copy of repository')
        
    if unpack_to_orig:
        print(' -- Unpacking existing repository into "orig" directory')
        shutil.unpack_archive(s_repo_name,orig_dir)
    completed()    

def get_external_files(download_ext=True):
    ext_name = os.path.join(ext_dir,'openff_ext_files.zip')
    if download_ext:
        try:
            print("This step may take several minutes. There are big files to transfer...")
            url = 'https://storage.googleapis.com/open-ff-common/openff_ext_files.zip'
            print(f'Downloading external files from {url}')
            urllib.request.urlretrieve(url, ext_name)
            print('Unpacking zip into "ext" directory')
            shutil.unpack_archive(ext_name,ext_dir)
            completed()
        except:
            completed(False,'Problem downloading external files!')
    else:
        completed(True,'Completed without new external download')

def download_raw_FF(download_FF=True):
    if download_FF:
        completed(fnbd.store_FF_bulk(newdir = work_dir,sources=orig_dir, archive=True, warn=True))
    else:
        fn = os.path.join(work_dir,'testData.zip')
        if os.path.isfile(fn):
            completed(True,'Completed using existing FF download')
        else:
            url = 'https://storage.googleapis.com/open-ff-common/repos/testData.zip'
            urllib.request.urlretrieve(url, fn)
            completed(True,'Copied previously downloaded FF dataset')
            
def create_master_raw_df(create_raw=True):
    if create_raw:
        rff = bdr.Read_FF(in_name='testData.zip', 
                          zipdir=work_dir,workdir = work_dir,
                          origdir=orig_dir,
                          flat_pickle = 'raw_flat.parquet')
        rff.import_raw()
        raw_df = get_raw_df(cols=['reckey'])
        # get number of records from old, repository data set
        # oldrecs = pd.read_pickle(os.path.join(orig_dir,'pickles','chemrecs.pkl'))
        oldrecs = get_df(os.path.join(orig_dir,'pickles','chemrecs.parquet'),
                        cols=['reckey'])
        if len(oldrecs)>len(raw_df):
            completed(False,'The old repository has MORE records than current download. Bad download??')
        else:
            completed(len(raw_df)>0)
    else:
        completed(True,'No action taken; new FF download skipped')
        
def update_upload_date_file():
    today = datetime.datetime.today()
    datefn= os.path.join(orig_dir,'curation_files','upload_dates.parquet')
    outdf = get_df(datefn)
    uklst = outdf.UploadKey.unique()
    
    df = get_raw_df(cols=['UploadKey','OperatorName'])
    ndf = df[~df.UploadKey.isin(uklst)].copy() # just the new ones
    
    gb = ndf.groupby('UploadKey',as_index=False)['OperatorName'].count()
    gb['date_added'] = today.strftime("%Y-%m-%d")
    gb.rename({'OperatorName':'num_records'}, inplace=True,axis=1)
    print(f'Number of new disclosures added to list: {len(gb)}')
    
    outdf.weekly_report = 'DONE'
    gb['weekly_report'] = 'NO'
    outdf = pd.concat([outdf,gb],sort=True)
    # outdf.to_csv(os.path.join(work_dir,'upload_dates.csv'),index=False)
    save_df(outdf,os.path.join(work_dir,'upload_dates.parquet'))    
    completed