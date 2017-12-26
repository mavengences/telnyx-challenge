# -*- coding: utf-8 -*-
"""
Created on Thu Dec 14 20:44:46 2017

@author: Dorien Xia
"""

import os 
import pandas as pd
import datetime
import json
import unittest

def add(x,y):
    return x+y

def load_file(file_name):
    current_directory=str(os.getcwd())
    loaded_df=pd.read_csv(current_directory+"\\"+file_name)
    return loaded_df 

def vlans_df_primary_port_sorter(file_name, port_ind):
    vlans_df=load_file(file_name)
    vlans_df_primary_port=vlans_df.loc[vlans_df['primary_port'] == port_ind].reset_index(drop=True).reset_index(drop=True)
    return vlans_df_primary_port
    if port_ind==1:
        print("vlans dataframe primary port is: \n")
        print(vlans_df_primary_port.head(10))
    elif port_ind==0:
        print("vlans dataframe non primary port is: \n")
        print(vlans_df_primary_port.head(10))
    
def redundant_df_loader_sorter(file_name, redund_ind):
    requests_df=load_file(file_name)
    print("requests dataframe is: \n")
    print(requests_df.head(10))
    redundant_df= requests_df.loc[requests_df['redundant'] == redund_ind].sort_values('request_id',ascending=True).reset_index(drop=True)
    return redundant_df
    if redund_ind==1:
        print("redundant dataframe is: \n")            
        print(redundant_df.head(10))
    elif redund_ind==0:
        print("non redundant dataframe is: \n")            
        print(redundant_df.head(10))
        
    

#current_directory=str(os.getcwd())
#requests_df=request_df_loader"test_requests.csv")
'''
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

'''
redundant_df=redundant_df_loader_sorter('test_requests.csv',1)
non_redundant_df=redundant_df_loader_sorter('test_requests.csv',0)
vlans_df_primary_port=vlans_df_primary_port_sorter('test_vlans.csv',1)
vlans_df_primary_port_used=vlans_df_primary_port_sorter('test_vlans.csv',1)
vlans_df_non_primary_port=vlans_df_primary_port_sorter('test_vlans.csv',0)

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
    json_2x_empty_df[i*2]['request_id']=json_redundant_df[i]['request_id']
    json_2x_empty_df[i*2]['redundant']=json_redundant_df[i]['redundant']
    json_2x_empty_df[i*2]['device_id']=min_vlans_device_df.iloc[0]['device_id']
    json_2x_empty_df[i*2]['vlan_id']=min_vlans_device_df.iloc[0]['vlan_id']
    json_2x_empty_df[i*2]['primary_port']=min_vlans_device_df.iloc[0]['primary_port']
    json_2x_empty_df[i*2+1]['request_id']=json_redundant_df[i]['request_id']
    json_2x_empty_df[i*2+1]['redundant']=json_redundant_df[i]['redundant']
    json_2x_empty_df[i*2+1]['device_id']=min_vlans_device_df.iloc[0]['device_id']
    json_2x_empty_df[i*2+1]['vlan_id']=min_vlans_device_df.iloc[0]['vlan_id']
    json_2x_empty_df[i*2+1]['primary_port']=0
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
        
final_redundant_df=pd.DataFrame(json_2x_empty_df)
print(final_redundant_df.head(10))
 
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

final_non_redundant_df=pd.DataFrame(json_non_redundant_df)
print(final_non_redundant_df.head(10))

final_output_df=final_redundant_df.append(final_non_redundant_df, ignore_index=True).sort_values('primary_port',ascending=True).sort_values('request_id',ascending=True).reset_index(drop=True).drop(['redundant'], axis=1)[['request_id','device_id','primary_port','vlan_id']]
print(final_output_df.head(10))
final_output_df.to_csv("test_final_output.csv", index = False)
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
