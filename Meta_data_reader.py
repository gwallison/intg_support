
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:15:03 2019

@author: GAllison

This module is used to read the meta data files in from a FracFocus zip 
of CSV files.

"""
import zipfile
import re
import pandas as pd
import numpy as np
import os


class Read_Meta():
    
    def __init__(self,in_name='currentData.zip',
                 working='./working/',outdir = './out/'):
        self.zname = os.path.join(working,in_name)
        #self.sources = sources
        self.out_dir = outdir
        self.working = working
        #self.missing_values = self.getMissingList()
        self.dropList = ['ClaimantCompany', 'DTMOD', 'Source'] # not used, speeds up processing
        self.cols_to_clean = ['Latitude','pKey']
        self.cols_to_lower = []
        # self.picklefn = os.path.join(working,flat_pickle)
        
    # def getMissingList(self):
    #     df = pd.read_csv(os.path.join(self.working,'curation_files',"missing_values.csv"),
    #                      quotechar='$',encoding='utf-8')
        
    #     # df = pd.read_csv(self.sources+ 'transformed/missing_values.csv',
    #     #                  quotechar='$',encoding='utf-8')

    #     return df.missing_value.tolist()
    
    # def get_api10(self,df):
    #     df['api10'] = df.APINumber.str[:10]
    #     return df
    

    # def get_density_from_comment(self,cmmt):
    #     """take a comment field and return density if it is present; there is a 
    #     common format"""
    #     if pd.isna(cmmt):
    #         return np.NaN
    #     if 'density' not in cmmt.lower():
    #         return np.NaN
    #     try:
    #         dens = re.findall(r"(\d*\.\d+|\d+)",cmmt)[0]
    #         return float(dens)
    #     except:
    #         return np.NaN
    
    # def make_date_fields(self,df):
    #     """Create the 'date' and 'year' fields from JobEndDate and correct errors"""

    #     # drop the time portion of the datatime
    #     df['d1'] = df.JobEndDate.str.split().str[0]
    #     # fix some obvious typos that cause hidden problems
    #     df['d2'] = df.d1.str.replace('3012','2012')
    #     df['d2'] = df.d2.str.replace('2103','2013')
    #     # instead of translating ALL records, just do uniques records ...
    #     tab = pd.DataFrame({'d2':list(df.d2.unique())})
    #     tab['date'] = pd.to_datetime(tab.d2)
    #     # ... then merge back to whole data set
    #     df = pd.merge(df,tab,on='d2',how='left',validate='m:1')
    #     df = df.drop(['d1','d2'],axis=1)
    #     df['year'] = df.date.dt.year
    #     return df
    
    def clean_cols(self,df,cols=[]):
        """FracFocus CSV data can sometimes include non-printing characters which
        are not intended and are a nuisance.  This funtion removes them from the
        given columns.  It is time intensive so is only performed on critical
        columns"""
        
        # if cols ==[]: 
        #     workcols = self.cols_to_clean
        # else:
        #     workcols = []
        #     for col in cols:
        #         if col in self.cols_to_clean:
        #             workcols.append(col)

        # for colname in workcols:
        #     print(f'   -- cleaning {colname}')
        #     # gb = df.groupby(colname,as_index=False)['pKey'].first()
        #     gb = df.groupby(colname,as_index=False).size()
        #     gb.columns = [colname,'junk']
        #     # replace return, newline, tab with single space
        #     gb['clean'] = gb[colname].replace(r'\r+|\n+|\t+',' ', regex=True)
        #     # remove whitespace from the ends
        #     gb.clean = gb.clean.str.strip()
        #     if colname in self.cols_to_lower:
        #         gb.clean = gb.clean.str.lower()
        #     df = pd.merge(df,gb,on=colname,how='left',validate='m:1')
        #     df.rename({colname:'oldRaw','clean':colname},axis=1,inplace=True)
        #     df.drop(['oldRaw','junk'],axis=1,inplace=True)
        # return df
    
        cols = df.columns.tolist()
        for col in cols:
            df[col] = df[col].str.strip()
        return df

    def import_raw(self):
        """
        """
        dflist = []
        with zipfile.ZipFile(self.zname) as z:
            inf = []
            for fn in z.namelist():
                # the files in the FF archive with the Ingredient records
                #  always start with this prefix...
                if fn[:15]=='registryupload_':
                    # need to extract number of file to correctly order them
                    num = int(re.search(r'\d+',fn).group())
                    inf.append((num,fn))
                    
            inf.sort()
            infiles = [x for _,x in inf]  # now we have a well-sorted list
            #print(self.startfile,self.endfile)
            for fn in infiles[0:]:
                with z.open(fn) as f:
                    print(f' -- processing {fn}')
                    t = pd.read_csv(f,low_memory=False,
                                    dtype={'APINumber':'str',
                                           'pKey':'str',
                                           'StateName':'str',
                                           'CountyName':'str',
                                           'Latitude':'str',
                                           'Longitude': 'str',
                                           'CountyNumber': 'str',
                                           'StateNumber': 'str',
                                           'TVD':'str',
                                           'TotalBaseWaterVolume':'str',
                                           'TotalBaseNonWaterVolume':'str',
                                           'WellName':'str',
                                           'JobEndDate':'str',
                                           'JobStartDate':'str',
                                           'OperatorName':'str',
                                           'Projection':'str'
                                           },
                                    )
                    t = t.filter(['APINumber', 'CountyName', 'CountyNumber', 
                                  'JobEndDate', 'JobStartDate', 'Latitude','Longitude', 'OperatorName', 'Projection',
                                  'StateName','StateNumber', 'TVD', 'TotalBaseNonWaterVolume', 'TotalBaseWaterVolume',
                                  'WellName', 'pKey'],axis=1)
                    dflist.append(t)
        final = pd.concat(dflist,sort=True)
        final.reset_index(drop=True,inplace=True) #  single integer as index
        final = self.clean_cols(final)
        return final
        
    def compile_from_set(self):
        mdf = pd.DataFrame({'pKey':[]})
        drop_lst = {}
        ldir = os.listdir(self.working)
        for fn in ldir:
            if fn[:11] == 'ff_archive_':
                fdate = fn[11:-4]
                print(fdate)
                # if (fdate < '2021-06-05') | (fdate > '2021-06-13'):
                #     continue
                self.zname = os.path.join(self.working,fn)
                ndf = self.import_raw()
                ndf['first_sited'] = fdate
                mg = pd.merge(mdf[['pKey']],
                              ndf[['pKey']],
                              on='pKey',how='outer',
                              indicator=True)
                dropped = mg[mg._merge=='left_only'].pKey.unique().tolist()
                for d in dropped:
                    if not d in drop_lst.keys():
                        drop_lst[d] = fdate
                        print('Dropped: ', d)  
                mdf = pd.concat([mdf,ndf],sort=True)
                mdf = mdf[~mdf[['APINumber', 'CountyName', 'CountyNumber', 
                  'JobEndDate', 'JobStartDate', 'Latitude','Longitude', 'OperatorName', 'Projection',
                  'StateName','StateNumber', 'TVD', 'TotalBaseNonWaterVolume', 'TotalBaseWaterVolume',
                  'WellName', 'pKey']].duplicated(keep='first')]
                print(len(mdf))
        gb = mdf.groupby('pKey',as_index=False).size().rename({'size':'num_disc'},axis=1)
        mdf = pd.merge(mdf,gb,on='pKey',how='left')
        # now add the removed disclosurs
        pKeys = []; dates = []
        for k in drop_lst:
            pKeys.append(k)
            dates.append(drop_lst[k])
        ddf = pd.DataFrame({'pKey':pKeys,
                            'removal_date':dates})
        mdf = pd.concat([mdf,ddf],sort=True)
        return mdf