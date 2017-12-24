# -*- coding: utf-8 -*-
"""
Created on Thu Dec 14 20:44:46 2017

@author: Dorien Xia
"""

import os 
import pandas as pd
import datetime
import json

current_directory=str(os.getcwd())
requests_df=pd.read_csv(current_directory+"\\test_requests.csv")
print("requests dataframe is: \n")
print(requests_df.head(10))

vlans_df=pd.read_csv(current_directory+"\\test_vlans.csv")
vlans_df_primary_port=vlans_df.loc[vlans_df['primary_port'] == 1]
vlans_df_non_primary_port=vlans_df.loc[vlans_df['primary_port'] == 0]
print("vlans dataframe primary port is: \n")
print(vlans_df_primary_port.head(10))
print("vlans dataframe non primary port is: \n")
print(vlans_df_non_primary_port.head(10))

redundant_df= requests_df.loc[requests_df['redundant'] == 1].sort_values('request_id',ascending=True)
non_redundant_df= requests_df.loc[requests_df['redundant'] == 0].sort_values('request_id',ascending=True)
print("redundant dataframe is: \n")            
print(redundant_df.head(10))
print("\n the length of the redundant dataframe is: "+str(len(redundant_df)))
print("non redundant dataframe is: \n") 
print(non_redundant_df.head(10))
print("\n the length of the non redundant dataframe is: "+str(len(non_redundant_df)))
print("\n the length of the original dataframe is: "+str(len(requests_df)))


vlans_df_primary_port = vlans_df_primary_port.reset_index(drop=True)
vlans_df_primary_port_used = vlans_df_primary_port
vlans_df_non_primary_port = vlans_df_non_primary_port.reset_index(drop=True)
#vlans_df_primary_port.to_csv("vlans_df_primary_port.csv")

json_non_redundant_df=json.loads(non_redundant_df.to_json(orient='records'))
json_redundant_df=json.loads(redundant_df.to_json(orient='records'))
#for i in range(0,10):


json_2x_empty_df=[]

for i in json_redundant_df:
    empty_dict={}
    json_2x_empty_df.append(i)
    json_2x_empty_df.append(empty_dict)
    
#json_2x_empty_df=json.loads(str(json_2x_empty_df))
for i in range(0,len(json_redundant_df),1):
    test_flag=False
    while test_flag==False:
        #print(json_non_redundant_df[i]
        min_vlans_df = vlans_df_primary_port[vlans_df_primary_port['vlan_id'] == min(vlans_df_primary_port['vlan_id'])]
        #print(min_vlans_df)
        min_vlans_device_df = min_vlans_df[min_vlans_df['device_id'] == min(min_vlans_df['device_id'])]
        test_df = vlans_df_non_primary_port[vlans_df_non_primary_port['vlan_id'] == min_vlans_device_df.iloc[0]['vlan_id']]
        test_df_device_id = vlans_df_non_primary_port[vlans_df_non_primary_port['device_id'] == min_vlans_device_df.iloc[0]['device_id']]
        if len(test_df_device_id)==0:
            vlans_df_primary_port=vlans_df_primary_port.drop(vlans_df_primary_port.index[min_vlans_device_df.index[0]])
            vlans_df_primary_port = vlans_df_primary_port.reset_index(drop=True)
            print("vlan id or device id does not exist for vlan id:"+str(min_vlans_device_df.iloc[0]['vlan_id'])+" in the non redundant data")
            test_flag=False
        elif len(test_df_device_id)>=1:
            print("vlan id and device id exists for vlan id:"+str(min_vlans_device_df.iloc[0]['vlan_id'])+" in the non redundant data")
            test_flag=True
    min_vlans_df = vlans_df_primary_port[vlans_df_primary_port['vlan_id'] == min(vlans_df_primary_port['vlan_id'])]
    min_vlans_device_df = min_vlans_df[min_vlans_df['device_id'] == min(min_vlans_df['device_id'])]
    json_2x_empty_df[i]['request_id']=json_redundant_df[i]['request_id']
    json_2x_empty_df[i]['redundant']=json_redundant_df[i]['redundant']
    json_2x_empty_df[i]['device_id']=min_vlans_device_df.iloc[0]['device_id']
    json_2x_empty_df[i]['vlan_id']=min_vlans_device_df.iloc[0]['vlan_id']
    json_2x_empty_df[i]['primary_port']=min_vlans_device_df.iloc[0]['primary_port']
    json_2x_empty_df[i+1]['request_id']=json_redundant_df[i]['request_id']
    json_2x_empty_df[i+1]['redundant']=json_redundant_df[i]['redundant']
    json_2x_empty_df[i+1]['device_id']=min_vlans_device_df.iloc[0]['device_id']
    json_2x_empty_df[i+1]['vlan_id']=min_vlans_device_df.iloc[0]['vlan_id']
    json_2x_empty_df[i+1]['primary_port']="0"
    #print(len(vlans_df_primary_port))
    #print('row you are deleting is:')
    #print(vlans_df_primary_port.iloc[min_vlans_device_df.index[0]])
    #print('index you are deleting is:')
    #print(min_vlans_device_df.index[0])
    vlans_df_primary_port=vlans_df_primary_port.drop(vlans_df_primary_port.index[min_vlans_device_df.index[0]])
    vlans_df_primary_port = vlans_df_primary_port.reset_index(drop=True)
    vlans_df_primary_port_used=vlans_df_primary_port_used.drop(vlans_df_primary_port_used.index[min_vlans_device_df.index[0]])
    vlans_df_primary_port_used = vlans_df_primary_port_used.reset_index(drop=True)
    #print(len(vlans_df_primary_port))
    del min_vlans_df
    del min_vlans_device_df
    if i < len(json_redundant_df):
        print(json_2x_empty_df[i])
        print(json_2x_empty_df[i+1])
        print("length of the primary port df used is:"+str(len(vlans_df_primary_port_used)))
 
#vlans_df_primary_port_used = vlans_df_primary_port_used.reset_index(drop=True)
       
for i in range(0,len(json_non_redundant_df)):
    #print(json_non_redundant_df[i])
    min_vlans_df = vlans_df_primary_port_used[vlans_df_primary_port_used['vlan_id'] == min(vlans_df_primary_port_used['vlan_id'])]
    #print(min_vlans_df)
    min_vlans_device_df = min_vlans_df[min_vlans_df['device_id'] == min(min_vlans_df['device_id'])]
    json_non_redundant_df[i]['device_id']=min_vlans_device_df.iloc[0]['device_id']
    json_non_redundant_df[i]['vlan_id']=min_vlans_device_df.iloc[0]['vlan_id']
    json_non_redundant_df[i]['primary_port']=min_vlans_device_df.iloc[0]['primary_port']
    #print(len(vlans_df_primary_port))
    #print('row you are deleting is:')
    #print(vlans_df_primary_port.iloc[min_vlans_device_df.index[0]])
    #print('index you are deleting is:')
    #print(min_vlans_device_df.index[0])
    vlans_df_primary_port_used=vlans_df_primary_port_used.drop(vlans_df_primary_port_used.index[min_vlans_device_df.index[0]])
    vlans_df_primary_port_used = vlans_df_primary_port_used.reset_index(drop=True)
    #print(len(vlans_df_primary_port))
    del min_vlans_df
    del min_vlans_device_df
    print(json_non_redundant_df[i])
#non_redundant_df['device_id']=''
#non_redundant_df['primary_port']=''
#non_redundant_df['vlan_id']=''
#final_output_df=pd.DataFrame(columns=['request_id','redundant','device_id'])

#for i in range(len(non_redundant_df)):
#for i in range(1,10):
#    final_output_df.loc[i]['request_id']=non_redundant_df.iloc[i]['request_id']
#    final_output_df.loc[i]['redundant']=non_redundant_df.iloc[i]['redundant']
#    final_output_df.loc[i]['device_id']='test'
    #row_to_append=pd.DataFrame(columns=['request_id','redundant','device_id'])
    #print(non_redundant_df.iloc[i]['request_id'])
    #row_to_append['request_id']=non_redundant_df.iloc[i]['request_id']
    #row_to_append['redundant']=non_redundant_df.iloc[i]['redundant']
    #row_to_append['device_id']='test'
    #print(row_to_append)
    #final_output_df.append(row_to_append)


#print("final_output_df is: \n")
#print(final_output_df)
'''
for index, row in non_redundant_df.iterrows():
    lowest_vlan_id_row=vlans_df_primary_port['vlan_id'].argmin()
    row['device_id']=vlans_df_primary_port.iloc[lowest_vlan_id_row]['device_id']
    row['primary_port']=vlans_df_primary_port.iloc[lowest_vlan_id_row]['primary_port']
    row['vlan_id']=vlans_df_primary_port.iloc[lowest_vlan_id_row]['vlan_id']
    vlans_df_primary_port.drop(vlans_df_primary_port.index[lowest_vlan_id_row])

print("non_redundant_df final is: \n")
print(non_redundant_df)

'''
