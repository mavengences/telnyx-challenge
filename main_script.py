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

# This code will define the files to load and whether or not they are test files

test_value="Y"

if test_value=="Y":
    string_to_prepend="test_"
elif test_value=="N":
    string_to_prepend=""

request_string_to_load=string_to_prepend+"requests.csv"
vlans_string_to_load=string_to_prepend+"vlans.csv"
final_output_string_to_write_to=string_to_prepend+"final_output.csv"

# This function will load a csv file defined above into a pandas dataframe

def load_file(file_name):
    current_directory=str(os.getcwd())
    loaded_df=pd.read_csv(current_directory+"\\"+file_name)
    return loaded_df 

# This function will load a vlans csv into a dataframe and sort the vlans dataframe by primary port vs non primary port

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

# This function will load a requests csv into a dataframe and sort the vlans dataframe by redundant request vs non redundant request
    
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

# this calls the initial script functionality to load various csv's into dataframes and sort values accordingly
        
redundant_df=redundant_df_loader_sorter(request_string_to_load,1)
non_redundant_df=redundant_df_loader_sorter(request_string_to_load,0)
vlans_df_primary_port=vlans_df_primary_port_sorter(vlans_string_to_load,1)
vlans_df_primary_port_used=vlans_df_primary_port_sorter(vlans_string_to_load,1)
vlans_df_non_primary_port=vlans_df_primary_port_sorter(vlans_string_to_load,0)

# this section of the script turns the requests dataframes into json for easier row iterations 

#for i in range(0,10):
json_redundant_df=json.loads(redundant_df.to_json(orient='records'))
json_non_redundant_df=json.loads(non_redundant_df.to_json(orient='records'))


# This function will parse the redundant dataframe to produce a file with two rows per request for redundant records matched with the vlans dataframe per the instructions

def redundant_df_parser(redundant_df, vlans_df_primary_port, vlans_df_primary_port_used, vlans_df_non_primary_port):
    #json_redundant_df=json.loads(redundant_df.to_json(orient='records'))
    # This step creates the jason 2x dataframe with every other row being empty
    json_2x_empty_df=[]
    for i in json_redundant_df:
        empty_dict={}
        json_2x_empty_df.append(i)
        json_2x_empty_df.append(empty_dict)
    #json_2x_empty_df=json.loads(str(json_2x_empty_df))
    # this for loop will iterate over the redundant dataframe to match requests with the vlans dataframe
    for i in range(0,len(json_redundant_df),1):
        test_flag=False
        # this while loop will check if a minimum vlan id and device id are avilable on the primary port & secondary port for a device
        # If the miniumum vlan id are not available for both primary and secondary ports, this functil will remove the record 
        # from the dataframe and check again for another minimum vlan id and device id
        while test_flag==False:
            #print(json_non_redundant_df[i]
            min_vlans_df = vlans_df_primary_port[vlans_df_primary_port['vlan_id'] == min(vlans_df_primary_port['vlan_id'])]
            #print(min_vlans_df)
            min_vlans_device_df = min_vlans_df[min_vlans_df['device_id'] == min(min_vlans_df['device_id'])]
            test_df = vlans_df_non_primary_port[vlans_df_non_primary_port['vlan_id'] == min_vlans_device_df.iloc[0]['vlan_id']]
            #print("test df is: \n")
            #print(test_df)
            test_df_device_id = test_df[test_df['device_id'] == min_vlans_device_df.iloc[0]['device_id']]
            if len(test_df_device_id)==0:
                vlans_df_primary_port=vlans_df_primary_port.drop(vlans_df_primary_port.index[min_vlans_device_df.index[0]])
                vlans_df_primary_port = vlans_df_primary_port.reset_index(drop=True)
                print("vlan id or device id does not exist for vlan id:"+str(min_vlans_device_df.iloc[0]['vlan_id'])+" in the non redundant data")
                test_flag=False
            elif len(test_df_device_id)>=1:
                print("vlan id and device id exists for vlan id:"+str(min_vlans_device_df.iloc[0]['vlan_id'])+" in the non redundant data")
                test_flag=True
                print("test device df id is: \n")
                print(test_df_device_id)
        min_vlans_df = vlans_df_primary_port[vlans_df_primary_port['vlan_id'] == min(vlans_df_primary_port['vlan_id'])]
        min_vlans_device_df = min_vlans_df[min_vlans_df['device_id'] == min(min_vlans_df['device_id'])]
        # this code will write the minimum dataframe value to the output json based on 2x row formatting
        json_2x_empty_df[i*2]['request_id']=json_redundant_df[i]['request_id']
        json_2x_empty_df[i*2]['redundant']=json_redundant_df[i]['redundant']
        json_2x_empty_df[i*2]['device_id']=min_vlans_device_df.iloc[0]['device_id']
        json_2x_empty_df[i*2]['vlan_id']=min_vlans_device_df.iloc[0]['vlan_id']
        json_2x_empty_df[i*2]['primary_port']=min_vlans_device_df.iloc[0]['primary_port']
        json_2x_empty_df[i*2+1]['request_id']=json_redundant_df[i]['request_id']
        json_2x_empty_df[i*2+1]['redundant']=json_redundant_df[i]['redundant']
        json_2x_empty_df[i*2+1]['device_id']=test_df_device_id.iloc[0]['device_id']
        json_2x_empty_df[i*2+1]['vlan_id']=test_df_device_id.iloc[0]['vlan_id']
        json_2x_empty_df[i*2+1]['primary_port']=test_df_device_id.iloc[0]['primary_port']
        #print(len(vlans_df_primary_port))
        #print('row you are deleting is:')
        #print(vlans_df_primary_port.iloc[min_vlans_device_df.index[0]])
        #print('index you are deleting is:')
        #print(min_vlans_device_df.index[0])
        # this code will remove the vlan matched with the device id for the for loop so that the same vlan id will not be used twice
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
    # this code will output the dataframes generated from this script to the main python module 
    final_list_to_return=[]
    final_list_to_return.append(vlans_df_primary_port)
    final_list_to_return.append(vlans_df_primary_port_used)
    final_list_to_return.append(vlans_df_non_primary_port)
    final_list_to_return.append(final_redundant_df)
    return final_list_to_return
    print(final_redundant_df.head(10))

# THis script calls the function above and assigns output to values	
final_output_redundant_parser_list=redundant_df_parser(json_redundant_df, vlans_df_primary_port, vlans_df_primary_port_used, vlans_df_non_primary_port)
vlans_df_primary_port=final_output_redundant_parser_list[0]
vlans_df_primary_port_used=final_output_redundant_parser_list[1]
vlans_df_non_primary_port=final_output_redundant_parser_list[2]
final_redundant_df=final_output_redundant_parser_list[3]
 
# This function will take the output from the redundant script above and apply logic to match vlan ids to non_redundant requests
#vlans_df_primary_port_used = vlans_df_primary_port_used.reset_index(drop=True)
def non_redundant_df_parser(non_redundant_df, vlans_df_primary_port, vlans_df_primary_port_used, vlans_df_non_primary_port):       
    #json_non_redundant_df=json.loads(non_redundant_df.to_json(orient='records'))
    # for loop to iterate over the entire redundant df
    for i in range(0,len(json_non_redundant_df)):
        #print(json_non_redundant_df[i])
        # logic used to determine minimum vlan id and device id for primary ports that were not used in the functions above
        min_vlans_df = vlans_df_primary_port_used[vlans_df_primary_port_used['vlan_id'] == min(vlans_df_primary_port_used['vlan_id'])]
        #print(min_vlans_df)
        min_vlans_device_df = min_vlans_df[min_vlans_df['device_id'] == min(min_vlans_df['device_id'])]
        # logic to write values to output json 
        json_non_redundant_df[i]['device_id']=min_vlans_device_df.iloc[0]['device_id']
        json_non_redundant_df[i]['vlan_id']=min_vlans_device_df.iloc[0]['vlan_id']
        json_non_redundant_df[i]['primary_port']=min_vlans_device_df.iloc[0]['primary_port']
        #print(len(vlans_df_primary_port))
        #print('row you are deleting is:')
        #print(vlans_df_primary_port.iloc[min_vlans_device_df.index[0]])
        #print('index you are deleting is:')
        #print(min_vlans_device_df.index[0])
        # logic used to remove primary port values so they are not recycled. 
        vlans_df_primary_port_used=vlans_df_primary_port_used.drop(vlans_df_primary_port_used.index[min_vlans_device_df.index[0]])
        vlans_df_primary_port_used = vlans_df_primary_port_used.reset_index(drop=True)
        #print(len(vlans_df_primary_port))
        del min_vlans_df
        del min_vlans_device_df
        print(json_non_redundant_df[i])
    final_non_redundant_df=pd.DataFrame(json_non_redundant_df)
    print(final_non_redundant_df.head(10))
    list_to_return=[]
    list_to_return.append(vlans_df_primary_port)
    list_to_return.append(vlans_df_primary_port_used)
    list_to_return.append(vlans_df_non_primary_port)
    list_to_return.append(final_non_redundant_df)
    return list_to_return

# this calls the function above and outputs final dataframes to the main python module
final_non_redundant_df_list=non_redundant_df_parser(non_redundant_df, vlans_df_primary_port, vlans_df_primary_port_used, vlans_df_non_primary_port)
vlans_df_primary_port=final_non_redundant_df_list[0]
vlans_df_primary_port_used=final_non_redundant_df_list[1]
vlans_df_non_primary_port=final_non_redundant_df_list[2]
final_non_redundant_df=final_non_redundant_df_list[3]

# these commands will format the final output dataframe to match the requirements in the pdf and write the final df to a csv file. 
final_output_df=final_redundant_df.append(final_non_redundant_df, ignore_index=True).sort_values(['request_id', 'primary_port', 'device_id'], ascending=[True, True, True]).reset_index(drop=True).drop(['redundant'], axis=1)[['request_id','device_id','primary_port','vlan_id']]
print(final_output_df.head(10))
final_output_df.to_csv(final_output_string_to_write_to, index = False)


