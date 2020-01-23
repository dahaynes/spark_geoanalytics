# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 09:21:28 2020

@author: dahaynes
"""
import psycopg2
from psycopg2 import extras
from collections import OrderedDict
import csv

class breast_cancer(object):
    
    def __init__(self, theConnectionDict, outFilePath, gridTableName, geoAggregateTableName=None, geoAggregateColumn=None, caseStatement=None):
        
        self.psqlConnectionDict = theConnectionDict
        self.gridTableName = gridTableName
        self.geoAggregateTableName = geoAggregateTableName
        self.geoAggregateColumn = geoAggregateColumn
        self.caseStatement = caseStatement
        try:
            self.psqlConn = self.CreateConnection(self.psqlConnectionDict)
        except:
            print("No Connection")
            self.psqlConn.close()
        else:
            self.psqlCur = self.psqlConn.cursor(cursor_factory=extras.DictCursor)
            self.maxGridRecords = self.GetGridMaxRecords(self.psqlCur, gridTableName)
            self.FilterQueries = self.BreastCancerUtilizationCalc(self.geoAggregateTableName, self.geoAggregateColumn, self.caseStatement)
            
            for p, f in enumerate(self.FilterQueries):
                print("Running filter query {} of {}".format(p+1, len(self.FilterQueries)) )
                #if p ==  0: print(f)
                try:
                    self.psqlCur.execute(f)
                except:
                    print("ERROR with query")
                    print(f)
                    break
                    
                else:
                    filters = self.psqlCur.fetchall()
                    outFilePath = "{}_{}.csv".format(outFilePath.split(".")[0], p)
                    self.WriteFile(outFilePath, filters)
        finally:     
            self.psqlConn.close()
            
            
            
        #self.gridRecords = self.CalculateGridIterations(self.psqlCur, gridTableName)
        
    
    def WriteFile(self, filePath, theDictionary):
        """
        This function writes out the dictionary as csv
        """
        
        theKeys = list(theDictionary[0].keys())
        print(theKeys)
        
        with open(filePath, 'w', newline='\n') as csvFile:
            fields = theKeys
            theWriter = csv.DictWriter(csvFile, fieldnames=fields, extrasaction='ignore', delimiter = ';')
            theWriter.writeheader()
    
            for rec in theDictionary:
                theWriter.writerow(rec)
    
    def PrintAllSQL(self, sqlList, pretty_print=False):
        """
        """
        self.CreateSQLStatements(pretty_print)
        
        for i in (sqlList):
            print(i)
            
    def PrettySQL(self, sqlList,indexValue):
        """
        Funtion to make pretty print sql
        """
        goodSQL = [sql.replace("\n", "" )for sql in sqlList]
        return ", \n".join(goodSQL[:-1]) + "\n {}".format( goodSQL[-1])
            
    def WriteSQLtoFile(self, sqlList, outFilePath):
        """
        Doc string
        """
        
        with open(outFilePath, "w", newline="\n") as fout:
            for i in sqlList:
                fout.write(i)
                
    def CreateConnection(self, psqlConnection):
        """
        This method will get a connection. Need to make sure that the DB is set correctly.
        """
    
        connection = psycopg2.connect(host=psqlConnection['host'], database=psqlConnection['db'], user=psqlConnection['user'])

        return connection
    

    def GetGridMaxRecords(self, psqlCur, gridTableName):
        """
        
        """
        query = """ SELECT count(1) as num_grid_points FROM {}""".format(gridTableName)

        try:
            psqlCur.execute(query)
        except:
            print(dir(psqlCur))
            print("\n", query)
        
        rec = psqlCur.fetchone()
        
        return(rec['num_grid_points'])
        
        
    def EligiblePopulationCalc(self):
        """
        
        """
        
        query  ="""WITH synthetic_people as
            (
            SELECT p.sp_hh_id, hh.geom as geom, p.sex, p.age, (hh.head_household_income - 30350 + (10800*hh.head_household_size)) as income, 1 as value, p.race
            FROM sage.synthetic_household hh INNER JOIN sage.synthetic_people p ON hh.household_id = p.sp_hh_id
            WHERE (hh.head_household_income - 30350 + (10800*hh.head_household_size) < 0 AND p.sex = 2 AND p.age >= 40)
             OR
            (p.sex = 2 AND p.age >= 40 AND p.race IN (3,4,5))
            )"""
        return(query)
        
        
    def CalculateUninsuredPoplation(self, caseStatement="CASE WHEN p.race IN (3,4,5) THEN 1 ELSE .1 END"):
        """
        This is where new case statements can be injected
        """
        query = """uninsured_population as
            (
            SELECT p.sp_hh_id, p.sex, p.age, p.race, p.income, p.value, geom,
            {} as expected_population
            FROM synthetic_people p
            ) """.format(caseStatement)
        
        return query
    
    def GridPartitioning(self, theStatementDictionary):
        """
        This is important
        
        """
        query = """grid as
            (
            SELECT g.gid, ST_Transform(geom,26915) as geom
            FROM {gridTableName} g
            WHERE g.gid BETWEEN {minGridID} AND {maxGridID}
            ) """.format(**theStatementDictionary)
        
        return(query)
    
    def AggregateEligiblePopulation(self, geogUnitColumnName="sp_hh_id",  geogAggregationStatement=""):
        """
        """
        theDict = {"geogColumnName": geogUnitColumnName, "aggregationStatement": geogAggregationStatement }
        
        query = """eligible_aggregation as
            (
            select c.{geogColumnName} as bound_id, st_transform(c.geom,26915) as geom,
            count(c.expected_population) as expected_population
            from uninsured_population c {aggregationStatement}
            group by c.{geogColumnName}, c.geom
            )""".format(**theDict)
        
        return query

    def BufferCalculation(self, populationThreshold=100):
        """
        """
        query = """ grid_person_join as
            (
            SELECT gid, g.geom, w.bound_id as bound_id, ST_Distance(g.geom, w.geom) as distance, w.expected_population, 1 as num_geog_features
            FROM grid g CROSS JOIN eligible_aggregation w
            ), grid_people as
            (
            SELECT gid, geom, distance, sum(num_geog_features) OVER w as num_geog_features, sum(expected_population) OVER w as total_sage_expected
            FROM grid_person_join
            WINDOW w AS (PARTITION BY gid, geom ORDER BY distance ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
            ), buffer_definition as
            (
            SELECT gid, geom, min(distance) as min_buffer_distance
            FROM grid_people
            WHERE total_sage_expected >= {}
            GROUP BY gid, geom
            ORDER BY gid
            )""".format(populationThreshold)
        
        return(query)
    
    def CalculateDenominator(self, ):
        """
        """
        
        query = """ denominator as
            (
            SELECT b.gid, b.geom, sum(expected_population) as expected_population, count(bound_id) as num_geographic_features
            FROM buffer_definition b 
            INNER JOIN eligible_aggregation p on ST_DWithin( b.geom,  p.geom, b.min_buffer_distance)
            GROUP BY b.gid, b.geom
            ORDER BY b.gid
            )"""
        
        return(query)



    def CalculateNumerator(self, geoAggregationTable=None, geogUnitColumnName=None):
        """
        """
        
        if geoAggregationTable:
            
            theDict = {"geogColumnName": geogUnitColumnName, "geoTable": geoAggregationTable }
            
            numeratorAggregator = """clients_aggregate as
            (
            SELECT b.{geogColumnName} as bound_id, count(c.client_id) as num_clients, ST_Transform(ST_Centroid(b.geom),26915) as geom
            FROM sage.breast_clients_2010_2015 c
            INNER JOIN {geogTableName} b on ST_Intersects(c.geom, b.geom)
            GROUP BY b.geogColumnName, b.geom
            ),""".format(**theDict)
        else:
            numeratorAggregator = """clients_aggregate as
            (
            SELECT c.gid as bound_id, count(c.client_id) as num_clients, ST_Transform(ST_Centroid(c.geom),26915) as geom
            FROM sage.breast_clients_2010_2015 c
            GROUP BY c.gid, c.geom
            ),"""
        
        query = """{} numerator as
            (
            SELECT b.gid, b.geom, sum(c.num_clients) as num_clients
            FROM buffer_definition b 
            INNER JOIN clients_aggregate c on ST_DWithin( b.geom,  c.geom, b.min_buffer_distance)
            GROUP BY b.gid, b.geom
            ORDER BY b.gid
            )""".format(numeratorAggregator,  )
        
        return(query)
    
    def FilterRelativeRiskCalculation(self,):
        """
        """
        query = """SELECT n.gid, ST_AsText(n.geom) as geom, num_clients as num, expected_population*5 as denom, 
            num_clients/(expected_population*5)::float as ratio, num_geographic_features as num_features
            FROM numerator n INNER JOIN denominator d ON(n.gid = d.gid)"""
        
        return(query)
    
    def BreastCancerUtilizationCalc(self, aggregationTable=None, aggregationTableColumnName=None, caseStatement=None):
        """ 
        This function will calculate the elgibile population
        """
        listOfQueries = []
        maxGridID = 1000
        sqlDict = {"gridTableName": self.gridTableName }
        for i in range(1, self.maxGridRecords, maxGridID):
            
            minGridID = i    
            sqlDict["minGridID"] = minGridID
            sqlDict["maxGridID"] = maxGridID
            sqlQueryList = []
            sqlQueryList.append(self.EligiblePopulationCalc())
            sqlQueryList.append(self.CalculateUninsuredPoplation(caseStatement))
            sqlQueryList.append(self.GridPartitioning(sqlDict))
            sqlQueryList.append(self.AggregateEligiblePopulation())
            sqlQueryList.append(self.BufferCalculation())
            sqlQueryList.append(self.CalculateDenominator())
            sqlQueryList.append(self.CalculateNumerator())
            sqlQueryList.append(self.FilterRelativeRiskCalculation())
            
    
            theQuery = self.PrettySQL(sqlQueryList, -1)
            #print(minGridID, maxGridID)
            
            maxGridID += 1000
            listOfQueries.append(theQuery)
        return(listOfQueries)
            