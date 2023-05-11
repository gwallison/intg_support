# -*- coding: utf-8 -*-
"""
Created on Fri May  5 18:26:56 2023

@author: garya

Used to guide the colab-type builder code.  Each routine runs some steps for a 
cell.
"""

# Preamble: run as soon as this module is imported

import os 
import shutil
import pandas as pd
from IPython.display import display
from IPython.display import Markdown as md
from intg_support.file_handlers import store_df_as_csv, save_df, get_df

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
    import datetime    
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
    import urllib.request
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
    import urllib.request
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

