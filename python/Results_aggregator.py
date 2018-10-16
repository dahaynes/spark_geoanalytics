#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 16 09:19:11 2018

Spark dataset builder

@author: david
"""
import os, csv


inputPath = r"/media/sf_data/sage_data/results/filter_joins_10percent"
outCSVPath = r"/media/sf_data/sage_data/results/filter_joins_10percent.csv"

allCSVPaths = [ "%s/%s" % (root, f) for root, dirs, files in os.walk(inputPath) for f in files if "crc" not in f and 'csv' in f ] 

with open(outCSVPath, 'w') as outF:
    theWriter = csv.writer(outF)
    for numCSV, csvPath in enumerate(allCSVPaths):
        with open(csvPath, 'r') as inCSV:
            theReader = csv.reader(inCSV)
            
            if numCSV == 0:
                for row in theReader:
                    theWriter.writerow(row)   

            else:
                next(theReader)
     
                for row in theReader:
                    theWriter.writerow(row)    
                
                
print("Done")
#for f in files if "csv" == f.split(".")[1]