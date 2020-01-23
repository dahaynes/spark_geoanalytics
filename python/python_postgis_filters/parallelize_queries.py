# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 09:32:25 2020

@author: dahaynes
"""

def Main(g):
    """
    
    """
#    print(g)
    
    theTableName = "mort_{}_{}_p{}".format(g["tableName"],  g["geographic_unit"], g["threshold"])
    print(theTableName)
    m = mortality_race(theTableName, g["base"], g["minorityCategory"], g["minorityGroups"], g["yearRange"], g["geographic_unit"], g["threshold"])
    #m = mortality(theTableName, g["lower"], g["upper"], g["yearRange"], g["geographic_unit"], g["threshold"])
    m.CreateSQLStatements()
#    m.PrintAllSQL()
    outFilePath = r"E:\git\disparities_mapping\sql\disparities_sql\{}.sql".format(theTableName)
    outShapeFilePath = r"E:\git\disparities_mapping\shapefiles\resulting_grids\{}.sql".format(theTableName)
    m.WriteSQLtoFile(m.sql_race, outFilePath)
    #m.WriteSQLtoFile(m.sql_race, outFilePath)
    
    psqlCommand = "psql -d disparities_mapping -U david -f {}".format(outFilePath)
    print(psqlCommand)
    p = subprocess.Popen(psqlCommand, shell=True)
    p.wait()
    out, err = p.communicate()
    
    psqlCommand = """pgsql2shp -f {} -h localhost -u david disparities_mapping "SELECT * FROM {} """.format(outShapeFilePath, theTableName)
    print(psqlCommand)
    p = subprocess.Popen(psqlCommand, shell=True)
    p.wait()
    out, err = p.communicate()
    
    
if __name__ == '__main__':
    pool = mp.Pool(10)
    theGroups = GetData()

    #asyncronous
    results = pool.map(Main, theGroups)
    pool.close()
    pool.join()