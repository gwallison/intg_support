import intg_support.geo_tools as gt
from intg_support.file_handlers import  get_df
import os
import pandas as pd
from itables import init_notebook_mode
init_notebook_mode(all_interactive=True)
from itables import show as iShow
import itables.options as opt
opt.classes="display compact cell-border"
opt.maxBytes = 0
opt.maxColumns = 0

work_dir = './tmp'
work_dir = ''
df = get_df(os.path.join(work_dir,'full_df.parquet'))
df = df[df.in_std_filtered]

def show_school_list():
    sch_df = pd.read_csv('https://storage.googleapis.com/gwa-test/all_states_school_hits.csv')
    gb1 = sch_df.groupby(['bgStateName','school_name'],as_index=False)['api10'].count()
    gb1 = gb1.reset_index(drop=True)
    gb1['school_index'] = gb1.index.astype(int)
    iShow(gb1)
    return gb1,sch_df

def get_lat_lon(gb1,sch_df,index=426):
    name = gb1[gb1.school_index==index].school_name.tolist()[0]
    t = sch_df[sch_df.school_name==name]
    lat = t.sc_lat.iloc[0]; lon = t.sc_lon.iloc[0]
    return lat,lon

def show_target_map(lat,lon,clickable=False):
    gt.show_simple_map(lat,lon,clickable)

def locate_wells(df,lat,lon,buffer_m=1609):
    wells = gt.make_as_well_gdf(df)
    apis = gt.find_wells_near_point(lat,lon,wells)#,buffer_m=200)
    print(f'Number of wells = {len(apis)}')
    return apis
    
def locate_wells_within_area(df,area_df,buffer_m=1609):
    wells = gt.make_as_well_gdf(df)
    apis = gt.find_wells_within_area(area_df,wells)#,buffer_m=200)
    print(f'Number of wells = {len(apis)}')
    return apis


    
def show_well_info(apis):
    t = df[df.api10.isin(apis)].copy()
    # t = t[t.date.dt.year>2022]
    dgb = t.groupby(['UploadKey'],as_index=False)[['date','api10','OperatorName','TotalBaseWaterVolume','ingKeyPresent']].first()
    iShow(dgb[['date','OperatorName','api10','TotalBaseWaterVolume','ingKeyPresent']])
    return t, dgb

def show_water_used(dgb):
    from pylab import gca, mpl
    import seaborn as sns
    sns.set_style("whitegrid")
    sns.set_palette("deep")
    ax = dgb.plot('date','TotalBaseWaterVolume',style='o',alpha=0.75,legend=None,
                 # ylim=(0,24000000)
                 )
    ax.set_title('Water Volume (gal) used in separate fracking events',fontsize=14)
    ax = gca().yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    
def show_chem_summary(t):
    import intg_support.construct_tables_for_display as ctfd

    cgb = t.groupby('bgCAS',as_index=False)['calcMass'].sum().sort_values('calcMass',ascending=False)
    cgb1 = t.groupby('bgCAS',as_index=False)['epa_pref_name'].first()
    mg = pd.merge(cgb,cgb1,on='bgCAS',how='left')
    # mg[['bgCAS','epa_pref_name','calcMass']]

    chem_df = ctfd.make_compact_chem_summary(t)
    # chem_df.sort_values('Total mass used (lbs)',ascending=False,inplace=True)
    iShow(chem_df.reset_index(drop=True),maxBytes=0,columnDefs=[{"width": "100px", "targets": 0}])