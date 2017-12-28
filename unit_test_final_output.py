# -*- coding: utf-8 -*-
"""
Created on Mon Dec 25 19:13:31 2017

@author: dorie
"""

import unittest
import json
import main_script

class TestOutput(unittest.TestCase):
    # This test will see if the laod function works for vlans csv
    def test_df_vlans_loader(self):
        result=main_script.load_file('test_vlans.csv')
        test_result=len(result)
        self.assertNotEqual(test_result, 0)
    
    # This test will see if the laod function works for requests csv    
    def test_df_requests_loader(self):
        result=main_script.load_file('test_requests.csv')
        test_result=len(result)
        self.assertNotEqual(test_result, 0)
    
    # This test will see if the primary port filter is working
    def test_primary_port_filter(self):
        result=main_script.vlans_df_primary_port_sorter('test_vlans.csv',1)
        test_result=len(result.loc[result['primary_port'] !=1])
        self.assertEqual(test_result,0)
    
    # This test will see if the non primary port filter is working
    def test_non_primary_port_filter(self):
        result=main_script.vlans_df_primary_port_sorter('test_vlans.csv',0)
        test_result=len(result.loc[result['primary_port'] !=0])
        self.assertEqual(test_result,0)
        #self.assertEqual(main_script.add(1,-2),-1)
    
    # This test will determine if the redundant filer is working    
    def test_redundant_df_filter(self):
        result=main_script.redundant_df_loader_sorter('test_requests.csv',1)
        test_result=len(result.loc[result['redundant'] !=1])
        self.assertEqual(test_result,0)
    
    # This test will determine if the non redundant filer is working  
    def test_non_redundant_df_filter(self):
        result=main_script.redundant_df_loader_sorter('test_requests.csv',0)
        test_result=len(result.loc[result['redundant'] !=0])
        self.assertEqual(test_result,0)
    
    # This test will determine if the vlans final df lengths are matching    
    def test_vlans_length(self):
        result_1=main_script.vlans_df_primary_port_sorter('test_vlans.csv',0)
        result_2=main_script.vlans_df_primary_port_sorter('test_vlans.csv',1)
        combined_length=len(result_1)+len(result_2)
        length_input=len(main_script.load_file('test_vlans.csv'))
        self.assertEqual(combined_length,length_input)
    
    # This test will determine if the requests final df lengths are matching       
    def test_requests_length(self):
        result_1=main_script.redundant_df_loader_sorter('test_requests.csv',0)
        result_2=main_script.redundant_df_loader_sorter('test_requests.csv',1)
        combined_length=len(result_1)+len(result_2)
        length_input=len(main_script.load_file('test_requests.csv'))
        self.assertEqual(combined_length,length_input)
    
    # This test will determine whether the parser final output dataframe lengths are matching
    def test_redundant_df_parser_length(self):
        length_non_redundant=len(main_script.redundant_df_loader_sorter('test_requests.csv',1))
        redundant_df=main_script.redundant_df_loader_sorter('test_requests.csv',1)
        non_redundant_df=main_script.redundant_df_loader_sorter('test_requests.csv',0)
        vlans_df_primary_port=main_script.vlans_df_primary_port_sorter('test_vlans.csv',1)
        length_original_primary_port_df=len(vlans_df_primary_port)
        vlans_df_primary_port_used=main_script.vlans_df_primary_port_sorter('test_vlans.csv',1)
        vlans_df_non_primary_port=main_script.vlans_df_primary_port_sorter('test_vlans.csv',0)
        #for i in range(0,10):
        json_redundant_df=json.loads(main_script.redundant_df.to_json(orient='records'))
        json_non_redundant_df=json.loads(non_redundant_df.to_json(orient='records'))
        final_output_redundant_parser_list=main_script.redundant_df_parser(json_redundant_df, vlans_df_primary_port, vlans_df_primary_port_used, vlans_df_non_primary_port)
        vlans_df_primary_port=final_output_redundant_parser_list[0]
        vlans_df_primary_port_used=final_output_redundant_parser_list[1]
        vlans_df_non_primary_port=final_output_redundant_parser_list[2]
        final_redundant_df=final_output_redundant_parser_list[3]
        length_final_redundant_df=len(final_redundant_df)
        length_vlans_df_primary_port_used=len(vlans_df_primary_port_used)
        expected_length=length_final_redundant_df/2+length_vlans_df_primary_port_used
        print("length of the primary_port original dataframe is : " + str(length_original_primary_port_df))
        print("length of the final output redundant dataframe is : " + str(length_final_redundant_df))
        print("length of the final output primary port used dataframe is : " + str(length_vlans_df_primary_port_used))
        print("length of the original dataframe should equal length of primary port used df plus length of redundant df divided by two")
        self.assertEqual(expected_length, length_original_primary_port_df)
    
    # This test will check to see if the final dataframe length matches the expected length based on the original dataframe length    
    def test_final_df_length(self):
        length_1=len(main_script.load_file('test_requests.csv'))
        length_non_redundant=len(main_script.redundant_df_loader_sorter('test_requests.csv',1))
        redundant_df=main_script.redundant_df_loader_sorter('test_requests.csv',1)
        non_redundant_df=main_script.redundant_df_loader_sorter('test_requests.csv',0)
        vlans_df_primary_port=main_script.vlans_df_primary_port_sorter('test_vlans.csv',1)
        vlans_df_primary_port_used=main_script.vlans_df_primary_port_sorter('test_vlans.csv',1)
        vlans_df_non_primary_port=main_script.vlans_df_primary_port_sorter('test_vlans.csv',0)
        #for i in range(0,10):
        json_redundant_df=json.loads(redundant_df.to_json(orient='records'))
        json_non_redundant_df=json.loads(non_redundant_df.to_json(orient='records'))
        final_output_redundant_parser_list=main_script.redundant_df_parser(json_redundant_df, vlans_df_primary_port, vlans_df_primary_port_used, vlans_df_non_primary_port)
        vlans_df_primary_port=final_output_redundant_parser_list[0]
        vlans_df_primary_port_used=final_output_redundant_parser_list[1]
        vlans_df_non_primary_port=final_output_redundant_parser_list[2]
        final_redundant_df=final_output_redundant_parser_list[3]
        final_non_redundant_df_list=main_script.non_redundant_df_parser(non_redundant_df, vlans_df_primary_port, vlans_df_primary_port_used, vlans_df_non_primary_port)
        vlans_df_primary_port=final_non_redundant_df_list[0]
        vlans_df_primary_port_used=final_non_redundant_df_list[1]
        vlans_df_non_primary_port=final_non_redundant_df_list[2]
        final_non_redundant_df=final_non_redundant_df_list[3]
        final_output_df=final_redundant_df.append(final_non_redundant_df, ignore_index=True).sort_values(['request_id', 'primary_port', 'device_id'], ascending=[True, True, True]).reset_index(drop=True).drop(['redundant'], axis=1)[['request_id','device_id','primary_port','vlan_id']]
        print(final_output_df.head(10))
        expected_length=length_1+length_non_redundant
        length_2=len(final_output_df)   
        print("length of the input dataframe is : " + str(length_1))
        print("length of the redundant dataframe is : " + str(length_non_redundant))
        print("length of the final output dataframe is : " + str(length_2))
        self.assertEqual(expected_length, length_2)
    
    # This test will determine whether or not the test output matching my script output dataframe value for value
    def test_final_df_values(self):
        redundant_df=main_script.redundant_df_loader_sorter('test_requests.csv',1)
        non_redundant_df=main_script.redundant_df_loader_sorter('test_requests.csv',0)
        vlans_df_primary_port=main_script.vlans_df_primary_port_sorter('test_vlans.csv',1)
        vlans_df_primary_port_used=main_script.vlans_df_primary_port_sorter('test_vlans.csv',1)
        vlans_df_non_primary_port=main_script.vlans_df_primary_port_sorter('test_vlans.csv',0)
        #for i in range(0,10):
        json_redundant_df=json.loads(redundant_df.to_json(orient='records'))
        json_non_redundant_df=json.loads(non_redundant_df.to_json(orient='records'))
        final_output_redundant_parser_list=main_script.redundant_df_parser(json_redundant_df, vlans_df_primary_port, vlans_df_primary_port_used, vlans_df_non_primary_port)
        vlans_df_primary_port=final_output_redundant_parser_list[0]
        vlans_df_primary_port_used=final_output_redundant_parser_list[1]
        vlans_df_non_primary_port=final_output_redundant_parser_list[2]
        final_redundant_df=final_output_redundant_parser_list[3]
        final_non_redundant_df_list=main_script.non_redundant_df_parser(non_redundant_df, vlans_df_primary_port, vlans_df_primary_port_used, vlans_df_non_primary_port)
        vlans_df_primary_port=final_non_redundant_df_list[0]
        vlans_df_primary_port_used=final_non_redundant_df_list[1]
        vlans_df_non_primary_port=final_non_redundant_df_list[2]
        final_non_redundant_df=final_non_redundant_df_list[3]
        final_output_df=final_redundant_df.append(final_non_redundant_df, ignore_index=True).sort_values(['request_id', 'primary_port', 'device_id'], ascending=[True, True, True]).reset_index(drop=True).drop(['redundant'], axis=1)[['request_id','device_id','primary_port','vlan_id']]
        test_output_df=main_script.load_file('test_output.csv')
        dataframe_test_value=test_output_df.equals(final_output_df)
        print("test output dataframe is: \n")
        print(test_output_df)
        print("actual output dataframe is: \n")
        print(final_output_df)
        self.assertEqual(dataframe_test_value, True)
     
        
if __name__== '__main__':
    unittest.main()
        
    