# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 15:07:21 2020

@author: dahaynes
"""

import pandas as pd
import geopandas
from osgeo import ogr
import glob
from shapely import wkt




theCSVFiles = r"E:\git\sage_spatial_analysis\comparison_manuscript\filters_v3\county_parallel_final_k_master.csv"

df = pd.read_csv(theCSVFiles, sep=";")

df['theGeom'] = df['geom'].apply(wkt.loads)


gdf = geopandas.GeoDataFrame(df, geometry='theGeom', crs)
gdf["theGeom"].set_crs = 26915

gdf.to_file(r"E:\git\sage_spatial_analysis\comparison_manuscript\filters_v3\test.shp")