# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 09:49:16 2020

@author: dahaynes
"""

from sage import breast_cancer
import haynes
import psycopg2
from psycopg2 import extras





#connection = psycopg2.connect(host=haynes.myConnection['host'], database=haynes.myConnection['db'], user=haynes.myConnection['user'])
#cur = connection.cursor(cursor_factory=extras.DictCursor)
#cur.execute("SELECT * FROM sage.mn_census_tracts")
#connection.close()

#b = breast_cancer(haynes.myConnection, r"E:\git\sage_spatial_analysis\comparison_manuscript\filters\zcta.csv", "sage.regular_5000_grid", geoAggregateType="zcta", geoAggregateColumn="geoid", caseStatement="CASE WHEN p.race IN (3,4,5) THEN 1 ELSE .1 END", geoAggregateTableName="sage.mn_zcta")
#b = breast_cancer(haynes.myConnection, r"E:\git\sage_spatial_analysis\comparison_manuscript\filters\tracts.csv", "sage.regular_5000_grid", geoAggregateType="tracts", geoAggregateColumn="gid", caseStatement="CASE WHEN p.race IN (3,4,5) THEN 1 ELSE .1 END", geoAggregateTableName="sage.mn_census_tracts")
#b = breast_cancer(haynes.myConnection, r"E:\git\sage_spatial_analysis\comparison_manuscript\filters\blocks.csv", "sage.regular_5000_grid", geoAggregateType="blocks", geoAggregateColumn="gid", caseStatement="CASE WHEN p.race IN (3,4,5) THEN 1 ELSE .1 END", geoAggregateTableName="sage.mn_blocks")



#b = breast_cancer(haynes.myConnection, r"E:\git\sage_spatial_analysis\comparison_manuscript\filters\ind.csv", "sage.regular_5000_grid", geoAggregateType="individual", geoAggregateColumn="sp_hh_id", caseStatement="CASE WHEN p.race IN (3,4,5) THEN 1 ELSE .1 END")
b = breast_cancer(haynes.myConnection, r"E:\git\sage_spatial_analysis\comparison_manuscript\filters\ind_uninsured.csv", "sage.regular_5000_grid", geoAggregateType="individual", geoAggregateColumn="sp_hh_id", caseStatement = "CASE WHEN p.race IN (3,4,5) THEN 1 ELSE .126 END")
b = breast_cancer(haynes.myConnection, r"E:\git\sage_spatial_analysis\comparison_manuscript\filters\ind_uninsured_underinsured.csv", "sage.regular_5000_grid", geoAggregateType="individual", geoAggregateColumn="sp_hh_id", caseStatement = "CASE WHEN p.race IN (3,4,5) THEN 1 ELSE .226 END")


b = breast_cancer(haynes.myConnection, r"E:\git\sage_spatial_analysis\comparison_manuscript\filters\ind_composite.csv", "sage.regular_5000_grid", geoAggregateType="individual", geoAggregateColumn="sp_hh_id", caseStatement="CASE WHEN p.race IN (3,4,5) THEN 1 ELSE .1 END", uninsuredField="composite")
b = breast_cancer(haynes.myConnection, r"E:\git\sage_spatial_analysis\comparison_manuscript\filters\ind_uninsured_income.csv", "sage.regular_5000_grid", geoAggregateType="individual", geoAggregateColumn="sp_hh_id", caseStatement="CASE WHEN p.race IN (3,4,5) THEN 1 ELSE .1 END", uninsuredField="uninsured_income")
b = breast_cancer(haynes.myConnection, r"E:\git\sage_spatial_analysis\comparison_manuscript\filters\ind_uninsured_race.csv", "sage.regular_5000_grid", geoAggregateType="individual", geoAggregateColumn="sp_hh_id", caseStatement="CASE WHEN p.race IN (3,4,5) THEN 1 ELSE .1 END", uninsuredField="uninsured_race")
b = breast_cancer(haynes.myConnection, r"E:\git\sage_spatial_analysis\comparison_manuscript\filters\ind_uninsured_age_sex.csv", "sage.regular_5000_grid", geoAggregateType="individual", geoAggregateColumn="sp_hh_id", caseStatement="CASE WHEN p.race IN (3,4,5) THEN 1 ELSE .1 END", uninsuredField="uninsured_age_sex")



