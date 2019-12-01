import glob, os
import gzip
import pandas as pd
import dask.dataframe as dd
from dask.delayed import delayed
from pandas.io.json import json_normalize
from db_con import VSFDatabase


def split_json(json_col):
	split_1 = str(json_col).replace('"',"").replace("\\","").replace("{","").replace("}","").split(",")
	split_2 = list(l.split(":") for l in split_1)
	split_df = pd.DataFrame(split_2, columns = ['Event','Prop']) 
	return split_df

col_names = ['mdn', 'app', 'amplitude_id', 'device_id', 'user_id', 'event_time',
       'client_event_time', 'client_upload_time', 'server_upload_time',
       'event_id', 'session_id', 'event_type', 'amplitude_event_type',
       'version_name', 'os_name', 'os_version', 'device_brand',
       'device_manufacturer', 'device_model', 'device_carrier', 'country',
       'language', 'location_lat', 'location_lng', 'ip_address',
       'event_properties', 'user_properties', 'region', 'city', 'dma',
       'device_family', 'device_type', 'platform', 'uuid', 'paying',
       'start_version', 'user_creation_time', 'library', 'idfa', 'adid']

fNme = '2019-05-20_testmdn_child_parent_rs.txt.gz'
delayed_dfs = delayed(pd.read_csv)(fNme, delimiter = '|')
ddf = dd.from_delayed(delayed_dfs)
master_df = pd.DataFrame(columns = col_names+['Event','Prop'] )

for key, value in ddf.iterrows():
	row_df = pd.DataFrame(data = [value.to_list()], columns = col_names )
	json_df = split_json(value['event_properties'])
	row_df['A'] = 1
	json_df['A'] = 1

	master_df = pd.concat([master_df, pd.merge(row_df, json_df, on = 'A').drop('A',1)])
# print(master_df.reset_index())








