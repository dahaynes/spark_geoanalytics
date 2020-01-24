# -*- coding: utf-8 -*-
"""
Created on Fri Jan 24 09:36:20 2020

@author: dahaynes
"""
import pandas

def WriteMasterFile(outMasterFilePath, listOfCSVs):
    """

    """
    print("appending multiple CSVs")
    listOfDataFrames = []
    for counter, d in enumerate(listOfCSVs):
        if counter == 0: 
            df = pandas.read_csv(d, delimiter=";")
        else:
            listOfDataFrames.append(pandas.read_csv(d, delimiter=";"))

    df.append(listOfDataFrames)
    print("writing master CSV")
    df.to_csv(outMasterFilePath, index=False) 
    
    
listofFiles = [r"E:\git\sage_spatial_analysis\comparison_manuscript\filters\ind_{}.csv".format(c) for c in range(0,10)]
theMasterFile = r"E:\git\sage_spatial_analysis\comparison_manuscript\filters\ind_master2.csv"

listOfDataFrames = []
for counter, d in enumerate(listofFiles):
    listOfDataFrames.append(pandas.read_csv(d, delimiter=";"))

df = pandas.concat(listOfDataFrames)
print("writing master CSV")
df.to_csv(theMasterFile, index=False) 



#WriteMasterFile(r"E:\git\sage_spatial_analysis\comparison_manuscript\filters\ind_master2.csv", listofFiles)