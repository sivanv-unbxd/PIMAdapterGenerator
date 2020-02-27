#!/usr/bin/env python
# coding: utf-8
adapter_id = "5e55375dfcbe020009e823c3"
org_id = "5a846024f4d84006237106def240b6bd"
url_prefix = "http://pimdev.unbxd.io/api"
auth_token ="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjo5LCJleHAiOjE1ODMyMzg5ODR9.DmxxCHOS8S4Rwax2upqRpJ6dYMdBY4zubXGVMiGbb8w"

import json 
import pandas as pd 
from pandas.io.json import json_normalize #package for flattening json in pandas df
import zipfile
import xml.etree.ElementTree as ET
import pandas as pd
import csv
import xlrd
import math
# import ijson
import requests

#Sample XLS
xlsx= "http://unbxd-pimento.s3.amazonaws.com/01bd43af97f2f30c51d8cd0e93ae7cfd/imports/sftpFiles/1572268680375_1572268679465_PIM_Items_with_Categories_4.xlsx"
ip = "http://unbxd-pimento.s3.amazonaws.com/e17f1f63216d021d5fcbd7933c311b72/imports/sftpFiles/1569224827308_1569224826547_Sample_Import_Data.xlsx"
az_kurta = "http://unbxd-pimento.s3.amazonaws.com/c83cfa4e2d7e19b47d1c92813a85bf55/imports/sftpFiles/1581609768372_1581609766688_AZ_kurtas.xlsm"



class XML2DataFrame:

    def __init__(self, xml_data):
        self.root = ET.XML(xml_data)

    def parse_root(self, root):
        """Return a list of dictionaries from the text and attributes of the
        children under this XML root."""
        return [self.parse_element(child) for child in root.getchildren()]

    def parse_element(self, element, parsed=None):
        """ Collect {key:attribute} and {tag:text} from thie XML
         element and all its children into a single dictionary of strings."""
        if parsed is None:
            parsed = dict()

        for key in element.keys():
            if key not in parsed:
                parsed[key] = element.attrib.get(key)
            if element.text:
                parsed[element.tag] = element.text                
            else:
                raise ValueError('duplicate attribute {0} at element {1}'.format(key, element.getroottree().getpath(element)))           

        """ Apply recursion"""
        for child in list(element):
            self.parse_element(child, parsed)
        return parsed

    def process_data(self):
        """ Initiate the root XML, parse it, and return a dataframe"""
        structure_data = self.parse_root(self.root)
        return pd.DataFrame(structure_data)


# ## Class for parsing file type and creating coprresponding pandas method

# In[288]:


class FileParser(object):
    def load(self,url):
        self.url = url
        self.file_type = url.split(".")[-1]
        print("The URL file type is : ", self.file_type)
        method_name='parse_'+self.file_type
        method=getattr(self,method_name,lambda :'Invalid')
        return method()
        
    def infer_schema(self):
        self.df.info()
        self.columns = list(self.df.columns.values.tolist()) 
        print("List of all columns are : ", self.columns)
        print("##### Pandas inferred Schema")
        pandas_schema = self.df.columns.to_series().groupby(self.df.dtypes).groups
        print(pandas_schema)
    def parse_xlsx(self):
        return self.parse_excel()
    def parse_xlsm(self):
        return self.parse_xlsm()
    def parse_xls(self):
        return self.parse_excel()
    def parse_csv(self):
        df = pd.read_csv(url,  sep=",", header=0)
    def parse_zip(self):
        zip = zipfile.ZipFile('filename.zip')

        # available files in the container
        print (zip.namelist())
        zip.open(zip.namelist()[0])
    def parse_tsv(self):
        df = pd.read_csv(url,  sep="\t", header=0)
    def parse_json(self):
        df = pd.read_json(url)
#         https://www.dataquest.io/blog/python-json-tutorial/
    def parse_xml(self):
        xml2df = XML2DataFrame(url)
        self.df = xml2df.process_data()
    def parse_txt(self):
        df = pd.read_csv(url,  sep=" ")
    def parse_tsv(self):
        df = pd.read_csv(url,  sep="\t", header=0)

    def parse_excel(self):
        xls = pd.ExcelFile(self.url)
        # Now you can list all sheets in the file
        sheets = xls.sheet_names;
        print("Sheets present in excel file are : " , sheets)
        self.df = pd.read_excel(xls, sheets[0])
        return self.infer_schema()
    
    def parse_xlsm(self):
        print("Pasring Amazon File in xlsm format")
        xls = pd.ExcelFile(self.url)
        # Now you can list all sheets in the file
        sheets = xls.sheet_names;
        valid_enum_values = pd.read_excel(xls, sheet_name="Valid Values", header=1)
        properties_list = pd.read_excel(xls, sheet_name="Data Definitions", header=0)
        return properties_list, valid_enum_values
