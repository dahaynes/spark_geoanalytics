#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 16 09:19:11 2018

Spark dataset builder

@author: david
"""
import os, csv

def argument_parser():
    """
    Parse arguments and return parser object
    """
    import argparse

    parser = argparse.ArgumentParser(description= "Script that aggregates the dumped output of the adaptive filters using Python3")    
    parser.add_argument("-i", required=True, help="Input directory of adaptive filters", dest="inputPath")    
    parser.add_argument("-o", help="Full path for the output csv file", dest="outCSVPath")
    
    return parser


def AggregateData(inputPath, outCSVPath=None):
    """

    """
    allCSVPaths = [ "%s/%s" % (root, f) for root, dirs, files in os.walk(inputPath) for f in files if "crc" not in f and 'csv' in f ] 

    if not outCSVPath: outCSVPath = "%s.csv" % (inputPath)

    with open(outCSVPath, 'w') as outF:
        theWriter = csv.writer(outF)
        for numCSV, csvPath in enumerate(allCSVPaths):
            with open(csvPath, 'r') as inCSV:
                theReader = csv.reader(inCSV)
                
                if numCSV == 0:
                    for row in theReader:
                        theWriter.writerow(row)   

                else:
                    next(theReader, None)
         
                    for row in theReader:
                        theWriter.writerow(row)    
                


if __name__ == '__main__':
    #Command Line Tool
    args = argument_parser().parse_args()
    # inputPath = r"/media/sf_data/sage_data/results/uninsured_race_10percent"
    # outCSVPath = r"/media/sf_data/sage_data/results/uninsured_race_10percent.csv"

    AggregateData(args.inputPath, args.outCSVPath)
    print("Finished")

