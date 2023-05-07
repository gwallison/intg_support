# -*- coding: utf-8 -*-
"""
Created on Wed May  5 14:22:20 2021

@author: Gary

Change on Mar 14, 2022: Remove SkyTruth archive from the standard_filtered
data set.
"""

import intg_support.Table_manager as c_tab
import pandas as pd
import os
#import zipfile
#import datetime
import intg_support.cas_tools as ct


# def modification_date(filename):
#     t = os.path.getmtime(filename)
#     return datetime.datetime.fromtimestamp(t)

def banner(text):
    print()
    print('*'*80)
    space = ' '*int((80 - len(text))/2)
    print(space,text,space)
    print('*'*80)


class Template_data_set():
    
    def __init__(self,bulk_fn='currentData',
                 sources='./sources/',
                 outdir='./out_dir/',
                 set_name = 'template',
                 #pkl_when_creating=True
                 ):
        self.set_name= set_name
        self.bulk_fn = bulk_fn
        self.sources = sources
        self.outdir = outdir
        self.pkldir = os.path.join(self.sources,'pickles')
        self.df = None
        self.t_man = c_tab.Table_constructor(sources = sources,
                                             outdir= outdir,
                                             pkldir=self.pkldir)
        self.table_date = self.t_man.get_table_creation_date()
        if self.table_date==False:
            banner(f"!!! Pickles for data tables don't exist for {self.bulk_fn}.")
            banner('      * Run "build_data_set.py" first *')
            exit()
        self.wC = {}
        self.choose_fields()
        

    def get_fn_list(self):
        s = set()
        for t in self.wC.keys():
            for c in self.wC[t]:
                s.add(c)
        return list(s)

    def add_fields_to_keep(self,field_dict = {'bgCAS':['is_on_TEDX']}):
        for table in field_dict.keys():
            for col in field_dict[table]:
                self.wC[table].add(col)
                
 
    def create_set(self):
        self.merge_tables()

    def get_set(self,verbose=True):
        if verbose:
            print('Creating data set from scratch...')
        self.t_man.load_pickled_tables()
        self.prep_for_creation()
        self.create_set()
        if verbose:
            print(f'Dataframe ***"{self.set_name}"***\n')
            print(self.df.info())
        print('saving to parquet format: full_df.parquet')
        self.df.to_parquet(os.path.join(self.outdir,'full_df.parquet'))
        return self.df
    
    def prep_for_creation(self):        
        # workTables: point to set of tables to manipulate for current data set
        self.make_work_tables()
        ct.na_check(self.wRec,txt=' prep_for_creation: records')
        ct.na_check(self.wDisc,txt=' prep_for_creation: disclosures')
            
    # def save_compressed(self):
    #     print(f' -- Saving < {self.set_name} > as compressed zip file.')
    #     self.t_man.load_pickled_tables()
    #     self.prep_for_creation()
    #     self.create_set()
    #     fn = self.set_name
    #     tmpfn = fn+'.csv'
    #     self.df.to_csv(tmpfn,index=False) # write in default directory for CodeOcean
    #     with zipfile.ZipFile(self.outdir+fn+'.zip','w') as z:
    #          z.write(tmpfn,compress_type=zipfile.ZIP_DEFLATED)
    #     os.remove(tmpfn)

    def choose_fields(self):
        pass
    def merge_tables(self):
        #print(f'in template merge: {self.wC}')
        pass
    def make_work_tables(self):
        pass


class Full_set(Template_data_set):
    def __init__(self,bulk_fn='currentData',
                 sources='./sources/',
                 outdir='./out_dir/',
                 #pkl_when_creating = False,
                 set_name='full_no_filter'):
        Template_data_set.__init__(self,bulk_fn=bulk_fn,
                           sources=sources,
                           outdir=outdir,
                           #pkl_when_creating=pkl_when_creating,
                           set_name=set_name)
    
    def keep_all_fields(self):
        # for all field, we must have access to all t_man tables, so must load them
        self.t_man.load_pickled_tables()
        self.wC = {}
        for t in self.t_man.tables.keys():
            self.wC[t] = set()
            for fn in list(self.t_man.tables[t].columns):
                self.wC[t].add(fn) 

    def make_work_tables(self):
        self.wDisc = self.t_man.tables['disclosures'].copy()
        self.wRec = self.t_man.tables['chemrecs'].copy()       
        self.wBgCAS = self.t_man.tables['bgCAS'].copy()


    def choose_fields(self):
        self.keep_all_fields()

    def merge_tables(self):
        #print(f'in full merge: {self.wC}')
        # print(f'wRec: {len(self.wRec)}')
        self.df = pd.merge(self.wDisc,
                           self.wRec,
                           on='UploadKey',
                           how='inner',validate='1:m')
        # print(f'df {len(self.df)}' )
        self.df = pd.merge(self.df,
                           self.wBgCAS,
                           on='bgCAS',
                           how='left',validate='m:1')        
        self.df['in_std_filtered'] = ~(self.df.is_duplicate)&\
                                     ~(self.df.dup_rec)
        # print(f'df {len(self.df)}' )
        # # needed for compatibility with previous versions
        # self.df['data_source'] = 'bulk'
        
