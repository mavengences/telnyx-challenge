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
import main_script_functions 

# This code will define the files to load and whether or not they are test files "N" for loading non-test Production files 
# "Y" for loading test files.

test_value="N"

if test_value=="Y":
    string_to_prepend="test_"
elif test_value=="N":
    string_to_prepend=""

request_string_to_load=string_to_prepend+"requests.csv"
vlans_string_to_load=string_to_prepend+"vlans.csv"
final_output_string_to_write_to=string_to_prepend+"final_output.csv"

# this code calls functions in "main_script_functions.py" to load various csv's into dataframes and sort values accordingly        
redundant_df=main_script_functions.redundant_df_loader_sorter(request_string_to_load,1)
non_redundant_df=main_script_functions.redundant_df_loader_sorter(request_string_to_load,0)
vlans_df_primary_port=main_script_functions.vlans_df_primary_port_sorter(vlans_string_to_load,1)
vlans_df_primary_port_used=main_script_functions.vlans_df_primary_port_sorter(vlans_string_to_load,1)
vlans_df_non_primary_port=main_script_functions.vlans_df_primary_port_sorter(vlans_string_to_load,0)

# this section of the script turns the requests dataframes from the steps above into json for easier row iterations 

json_redundant_df=json.loads(redundant_df.to_json(orient='records'))
json_non_redundant_df=json.loads(non_redundant_df.to_json(orient='records'))

# This code parses the redundant requests by calling functions in "main_script_functions.py" and assigning output to values	
final_output_redundant_parser_list=main_script_functions.redundant_df_parser(json_redundant_df, vlans_df_primary_port, vlans_df_primary_port_used, vlans_df_non_primary_port)
vlans_df_primary_port=final_output_redundant_parser_list[0]
vlans_df_primary_port_used=final_output_redundant_parser_list[1]
vlans_df_non_primary_port=final_output_redundant_parser_list[2]
final_redundant_df=final_output_redundant_parser_list[3]

# This code takes in arguments from above and parses the non redundant requests by calling functions in "main_script_functions.py" and assigning output to values	
final_non_redundant_df_list=main_script_functions.non_redundant_df_parser(json_non_redundant_df, vlans_df_primary_port, vlans_df_primary_port_used, vlans_df_non_primary_port)
vlans_df_primary_port=final_non_redundant_df_list[0]
vlans_df_primary_port_used=final_non_redundant_df_list[1]
vlans_df_non_primary_port=final_non_redundant_df_list[2]
final_non_redundant_df=final_non_redundant_df_list[3]

# these commands will format the final output dataframe to match the requirements in the pdf and write the final df to a csv file. 
final_output_df=final_redundant_df.append(final_non_redundant_df, ignore_index=True).sort_values(['request_id', 'primary_port', 'device_id'], ascending=[True, True, True]).reset_index(drop=True).drop(['redundant'], axis=1)[['request_id','device_id','primary_port','vlan_id']]
final_output_df.to_csv(final_output_string_to_write_to, index = False)
print("All Finished Script Executed Successfully")
print("Final output has : "+str(len(final_output_df))+" records")
print("Sample 10 records can be seen below:")
print(final_output_df.head(10))
