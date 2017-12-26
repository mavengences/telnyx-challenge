# -*- coding: utf-8 -*-
"""
Created on Mon Dec 25 19:13:31 2017

@author: dorie
"""

import unittest
import main_script

class TestOutput(unittest.TestCase):
    def test_df_vlans_loader(self):
        result=main_script.load_file('test_vlans.csv')
        test_result=len(result)
        self.assertNotEqual(test_result, 0)
        
    def test_df_requests_loader(self):
        result=main_script.load_file('test_requests.csv')
        test_result=len(result)
        self.assertNotEqual(test_result, 0)
    
    def test_primary_port_filter(self):
        result=main_script.vlans_df_primary_port_sorter('test_vlans.csv',1)
        test_result=len(result.loc[result['primary_port'] !=1])
        self.assertEqual(test_result,0)

    def test_non_primary_port_filter(self):
        result=main_script.vlans_df_primary_port_sorter('test_vlans.csv',0)
        test_result=len(result.loc[result['primary_port'] !=0])
        self.assertEqual(test_result,0)
        #self.assertEqual(main_script.add(1,-2),-1)
        
    def test_redundant_df_filter(self):
        result=redundant_df_loader_sorter('test_requests.csv',1)
        test_result=len(result.loc[result['redundant'] !=1])
        self.assertEqual(test_result,0)
        
    def test_non_redundant_df_filter(self):
        result=redundant_df_loader_sorter('test_requests.csv',0)
        test_result=len(result.loc[result['redundant'] !=0])
        self.assertEqual(test_result,0)
        
    def test_vlans_length(self):
        result_1=vlans_df_primary_port_sorter('test_vlans.csv',0)
        result_2=vlans_df_primary_port_sorter('test_vlans.csv',1)
        combined_length=len(result_1)+len(result_2)
        length_input=len(main_script.load_file('test_vlans.csv'))
        self.assertEqual(combined_length,length_input)
        
    def test_requests_length(self):
        result_1=redundant_df_loader_sorter('test_requests.csv',0)
        result_2=redundant_df_loader_sorter('test_requests.csv',1)
        combined_length=len(result_1)+len(result_2)
        length_input=len(main_script.load_file('test_requests.csv'))
        self.assertEqual(combined_length,length_input)
        
        
if __name__== '__main__':
    unittest.main()
        
    