# -*- coding: utf-8 -*-
"""
Created on Fri May  5 18:26:56 2023

@author: garya

Used to guide the colab-type builder code.  Each routine runs some steps for a 
cell.
"""

# Preamble: run as soon as this module is imported

import os 
from IPython.display import display
from intg_support.file_handlers import  get_df
from intg_support.fetch_files import fetch_repo_full_df
from intg_support.common import round_sig

use_itables = True



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
       
        

def get_fulldf(work_dir='./tmp'):
    fetch_repo_full_df(outdir = work_dir,repo='keep_proprietary_paper_data')
    return get_df(os.path.join(work_dir,'full_df.parquet'))
    
