# -*- coding: utf-8 -*-
"""
Created on Wed Nov 11 21:43:06 2020

@author: dahaynes
"""

from sage import breast_cancer
import haynes
import psycopg2
from psycopg2 import extras
from collections import OrderedDict
import csv, os, glob, pandas
import multiprocessing as mp




#connection = psycopg2.connect(host=haynes.myConnection['host'], database=haynes.myConnection['db'], user=haynes.myConnection['user'])
#cur = connection.cursor(cursor_factory=extras.DictCursor)
#cur.execute("SELECT * FROM sage.mn_census_tracts")
#connection.close()






if __name__ == '__main__':
#    b = breast_cancer(haynes.myConnection, r"E:\work\county_parallel_final_k.csv", "sage.regular_5000_grid", \
#                  geoAggregateType="county", geoAggregateColumn="geoid", caseStatement="CASE WHEN p.race IN (3,4,5) THEN 1 ELSE .226 END", geoAggregateTableName="sage.mn_counties")
#    
    # b = breast_cancer(haynes.myConnection, r"E:\git\sage_spatial_analysis\comparison_manuscript\filters_v4\county_final.csv", "sage.regular_5000_grid", \
    #               geoAggregateType="county", geoAggregateColumn="geoid", caseStatement="CASE WHEN p.race IN (3,4,5) THEN 1 ELSE 1 END", \
    #               geoAggregateTableName="sage.mn_counties", parallelAnalysis=False)

    b = breast_cancer(haynes.myConnection, r"E:\git\sage_spatial_analysis\comparison_manuscript\filters_v4\county_final_parallel.csv", "sage.regular_5000_grid", \
                  geoAggregateType="county", geoAggregateColumn="geoid", caseStatement="CASE WHEN p.race IN (3,4,5) THEN 1 ELSE 1 END", \
                  geoAggregateTableName="sage.mn_counties", parallelAnalysis=True)

    b = breast_cancer(haynes.myConnection, r"E:\git\sage_spatial_analysis\comparison_manuscript\filters_v4\zcta_final_parallel.csv", "sage.regular_5000_grid", \
                      geoAggregateType="zcta", geoAggregateColumn="geoid10", caseStatement="CASE WHEN p.race IN (3,4,5) THEN 1 ELSE 1 END", \
                      geoAggregateTableName="sage.mn_zcta", parallelAnalysis=True)
    
    b = breast_cancer(haynes.myConnection,  r"E:\git\sage_spatial_analysis\comparison_manuscript\filters_v4\tracts_final_parallel.csv", "sage.regular_5000_grid",\
                      geoAggregateType="tracts", geoAggregateColumn="gid", caseStatement="CASE WHEN p.race IN (3,4,5) THEN 1 ELSE 1 END", \
                      geoAggregateTableName="sage.mn_census_tracts", parallelAnalysis=True)
    
    b = breast_cancer(haynes.myConnection, r"E:\git\sage_spatial_analysis\comparison_manuscript\filters_v4\blocks_final_parallel.csv", "sage.regular_5000_grid",\
                      geoAggregateType="blocks", geoAggregateColumn="gid", caseStatement="CASE WHEN p.race IN (3,4,5) THEN 1 ELSE 1 END", \
                      geoAggregateTableName="sage.mn_blocks", parallelAnalysis=True)
#    
    b = breast_cancer(haynes.myConnection, r"E:\git\sage_spatial_analysis\comparison_manuscript\filters_v4\individual_final_parallel.csv", "sage.regular_5000_grid",\
                      geoAggregateType="individual", geoAggregateColumn="sp_hh_id", caseStatement="CASE WHEN p.race IN (3,4,5) THEN 1 ELSE 1 END",\
                      geoAggregateTableName=None, parallelAnalysis=True)

    
#    parallelAnalysis()