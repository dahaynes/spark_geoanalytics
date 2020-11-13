# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 09:21:28 2020

@author: dahaynes
"""

import psycopg2
from psycopg2 import extras
from collections import OrderedDict
import csv, os, glob, pandas
import multiprocessing as mp


class breast_cancer(object):
    
    def __init__(self, theConnectionDict, outFilePath, gridTableName, geoAggregateType, \
                 geoAggregateColumn, caseStatement, \
                 geoAggregateTableName=None, uninsuredField=None, parallelAnalysis=True):
        
        self.psqlConnectionDict = theConnectionDict
        self.gridTableName = gridTableName
        self.geoAggregateType = geoAggregateType
        self.geoAggregateTableName = geoAggregateTableName
        self.geoAggregateColumn = geoAggregateColumn
        self.caseStatement = caseStatement
        self.uninsuredField = uninsuredField
        self.outDirectory = "\\".join(outFilePath.split("\\")[:-1])
        self.outFilePath = outFilePath
        self.parallelAnalysis = parallelAnalysis
        self.spatialFilterQueries = self.Main()
        
        
        if self.parallelAnalysis:
            self.parallelQueries()
#        self.parallelAnalysis()
            
            
    def Main(self,):
        """
        
        """
        try:
            self.psqlConn = self.CreateConnection(self.psqlConnectionDict)
        except:
            print("No Connection")
            self.psqlConn.close()
        else:
            self.psqlCur = self.psqlConn.cursor(cursor_factory=extras.DictCursor)
            self.maxGridRecords = self.GetGridMaxRecords(self.psqlCur, self.gridTableName)
            
            self.FilterQueries = self.BreastCancerUtilizationCalc(self.geoAggregateType,self.geoAggregateTableName,\
                                                                  self.geoAggregateColumn, self.caseStatement, self.uninsuredField)
            
            
            self.outCSVs = []
            if self.parallelAnalysis:
                return (self.FilterQueries)
        
            for p, f in enumerate(self.FilterQueries):
                print("Running filter query {} of {}".format(p+1, len(self.FilterQueries)) )
                if p ==  0: print(f)
                try:
                    self.psqlCur.execute(f)
                    pass
                except:
                    print("ERROR with query")
                    #print(f)
                    #break
                    
                else:
                    
                    filters = self.psqlCur.fetchall()
                    thePath = "{}_{}.csv".format(self.outFilePath.split(".")[0], p)
                    outFilePath = os.path.join(self.outDirectory, thePath)
                    self.outCSVs.append(outFilePath)
                    print("Writing to: ", outFilePath)
                    self.WriteFile(outFilePath, filters)
                    
                    thePath = "{}_{}.sql".format(self.outFilePath.split(".")[0],p)
                    outFilePath = os.path.join(self.outDirectory, thePath)
                    print("Writing to: ", outFilePath)
                            
                    with open(outFilePath, 'a+') as fout:
                        fout.write(f)
                        fout.write("\n---------------------------------\n")

                    
        finally:     
            self.psqlConn.close()
            masterFilePath = os.path.join(self.outDirectory, "{}_{}.csv".format(self.outFilePath.split(".")[0],"master") )
            if len(self.outCSVs) >1:
                self.WriteMasterFile(masterFilePath, self.outCSVs)
            print("Finished")
    
    
    def queryAnalysis(self, inQuery):
        """
        Worker function for the parallel Query
        """
        
        psqlConn = self.CreateConnection(self.psqlConnectionDict)
        if psqlConn:
            print("connection with DB")
            psqlCur = psqlConn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            try:
                psqlCur.execute(inQuery)
            except:
                print("********ERROR with query ******* \n")
                print(inQuery)
            else:
                return psqlCur.fetchall()
            finally:
                psqlCur.close()
                psqlConn.close()    
            
        
    
    def parallelQueries(self, numCores=10):
        """
        
        """
        pool = mp.Pool(len(self.spatialFilterQueries))
        print("Performing parallel analysis with {} cores".format(len(self.spatialFilterQueries)))
        try:
            results = pool.imap(self.queryAnalysis, self.spatialFilterQueries )
        except:
            print(mp.get_logger())
        
        pool.close()
        pool.join()
        

        thePath = "{}.sql".format(self.outFilePath.split(".")[0])
        outFilePath = os.path.join(self.outDirectory, thePath)
        print("Writing to: ", outFilePath)
        with open(outFilePath, 'w') as fout:
            for f in self.spatialFilterQueries:                   
                fout.write(f)
                fout.write("\n---------------------------------\n")        

        
        self.outCSVs = []
        for p, r in enumerate(results):
            thePath = "{}_{}.csv".format(self.outFilePath.split(".")[0], p)
            outFilePath = os.path.join(self.outDirectory, thePath)
            self.WriteFilters(outFilePath, r)
            self.outCSVs.append(outFilePath)
#       
            
        masterFilePath = os.path.join(self.outDirectory, "{}_{}.csv".format(self.outFilePath.split(".")[0],"master") )
        if len(self.outCSVs) >1:
            print("Writing results to {}".format(masterFilePath))
            self.WriteMasterFile(masterFilePath, self.outCSVs)
        print("Finished")


    
    def WriteFilters(self, filePath, theRecords):
        """
        This function writes out the records as csv
        """
    
        
        with open(filePath, 'a', newline='\n') as csvFile:
            theWriter = csv.writer(csvFile, delimiter=";")
#            print(theRecords)
            for r in theRecords:
                print(r)
                theWriter.writerow(r)
    
    
#    def WriteRecords(self, filePath, theDictionary):
#        """
#        This function writes out the dictionary as csv
#        """
#        
#        thekeys = list(theDictionary.keys())
#        
#        with open(filePath, 'a', newline='\n') as csvFile:
#            fields = list(theDictionary[thekeys[0]].keys())
#            theWriter = csv.DictWriter(csvFile, fieldnames=fields)
#            theWriter.writeheader()
#    
#            for k in theDictionary.keys():
#                theWriter.writerow(theDictionary[k])
            
    
    def WriteMasterFile(self,outMasterFilePath, listOfCSVs):
        """

        """
        print("appending multiple CSVs")
        listOfDataFrames = []
        for counter, d in enumerate(listOfCSVs):
            listOfDataFrames.append(pandas.read_csv(d, delimiter=";"))

        df = pandas.concat(listOfDataFrames)
        print("writing master CSV")
        df.to_csv(outMasterFilePath, index=False) 

    
    def WriteFile(self, filePath, theDictionary):
        """
        This function writes out the dictionary as csv
        """
        
        theKeys = list(theDictionary[0].keys())
        #print(theKeys)
        
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
        
        
    def CalculateUninsuredPoplation(self, caseStatement="CASE WHEN p.race IN (3,4,5) THEN 1 ELSE 1 END", uninsuredColumn=""):
        """
        This is where new case statements can be injected
        """
        if not uninsuredColumn:
            query = """uninsured_population as
                (
                SELECT p.sp_hh_id, p.sex, p.age, p.race, p.income, p.value, geom,
                {} as expected_population
                FROM synthetic_people p
                ) """.format(caseStatement)
            
            
        else:
            print("Calculating Distance Matrix for column: {}".format(uninsuredColumn))
            caseStatements = self.InsuranceCaseStatements()
            query = """
                insurance_datasets as
                (
                SELECT *
                FROM sage.mn_census_tracts t
                INNER JOIN sage.uninsured_age_sex_tracts a ON (a.id2 = t.geoid::bigint)
                INNER JOIN sage.uninsured_income_tracts i ON (i.id2 = t.geoid::bigint)
                INNER JOIN sage.uninsured_race_tracts r ON (r.id2 = t.geoid::bigint)
                ),
                uninsured_population_data as
                (
                SELECT p.sp_hh_id, p.sex, p.age, p.race, p.income, p.value, p.geom,
                {}
                FROM synthetic_people p INNER JOIN insurance_datasets i ON ST_Intersects(p.geom, i.geom)
                ), uninsured_tabulated as
                (
                SELECT sp_hh_id, sex, race, age, income, value, geom,
                (value_35_44 + value_45_54 + value_55_64 + value_65_74 + value_75 + state) as uninsured_age_sex,
                (value_under_25000 + value_25000_50000 + value_50000_75000 + value_50000_75000 + value_75000_100000 + value_over_100000 + state) as uninsured_income,
                (value_white_nonhispanic + value_black + value_asian + value_pacific_islander + value_sor + value_two_races + state) as uninsured_race,
                (value_35_44 + value_45_54 + value_55_64 + value_65_74 + value_75 + value_under_25000 + value_25000_50000 + value_50000_75000 +
                value_50000_75000 + value_75000_100000 + value_over_100000 + value_white_nonhispanic + value_black + value_asian +
                value_pacific_islander + value_sor + value_two_races + state ) as uninsured_composite,
                state, state_uninsured, state_uninsured_underinsured
                FROM uninsured_population_data
                ),
                uninsured_population as
                (
                SELECT sp_hh_id, sex, race, age, income, value, geom, 
                {} as expected_population
                FROM uninsured_tabulated
                )
                """.format(caseStatements, uninsuredColumn)
                
        
        return query

    def InsuranceCaseStatements(self):
        """
        """
        statements= """
            CASE WHEN p.age >= 35 AND p.age <= 44 AND p.race NOT IN (3,4,5) THEN value*i.per_f35_44yearsnoinsurance ELSE 0 END as value_35_44,
            CASE WHEN p.age >= 45 AND p.age <= 54 AND p.race NOT IN (3,4,5) THEN value*i.per_f45_54yearsnoinsurance ELSE 0 END as value_45_54,
            CASE WHEN p.age >= 55 AND p.age <= 64 AND p.race NOT IN (3,4,5) THEN value*i.per_f55_64yearsnoinsurance ELSE 0 END as value_55_64,
            CASE WHEN p.age >= 65 AND p.age <= 74 AND p.race NOT IN (3,4,5) THEN value*i.per_f65_74yearsnoinsurance ELSE 0 END as value_65_74,
            CASE WHEN p.age >= 75 THEN value*i.per_f75yearsandovernoinsurance ELSE 0 END as value_75,
            CASE WHEN p.income < 25000 THEN value*i.per_under25000noinsurance ELSE 0 END as value_under_25000,
            CASE WHEN p.income >= 25000 AND p.income <= 49999 THEN value*i.per_25000_49999noinsurance ELSE 0 END as value_25000_50000,
            CASE WHEN p.income >= 50000 AND p.income <= 74999 THEN value*i.per_50000_74999noinsurance ELSE 0 END as value_50000_75000,
            CASE WHEN p.income >= 75000 AND p.income <= 99999 THEN value*i.per_75000_99999noinsurance ELSE 0 END as value_75000_100000,
            CASE WHEN p.income >= 100000 THEN value*i.per_100000ormorenoinsurance ELSE 0 END as value_over_100000,
            CASE WHEN p.race = 1 THEN value*i.perUninsured_White_notHispanic ELSE 0 END as value_white_nonhispanic,
            CASE WHEN p.race = 2 THEN value*i.perUninsured_Black ELSE 0 END as value_black,
            CASE WHEN p.race = 6 THEN value*i.perUninsured_Asian ELSE 0 END as value_asian,
            CASE WHEN p.race = 7 THEN value*i.perUninsured_PacificIslander ELSE 0 END as value_pacific_islander,
            CASE WHEN p.race = 8 THEN value*i.perUninsured_SOR ELSE 0 END as value_sor,
            CASE WHEN p.race = 9 THEN value*i.perUninsured_TwoRaces ELSE 0 END as value_two_races,
            CASE WHEN p.race IN (3,4,5) THEN 1 ELSE .1 END as state,
            CASE WHEN p.race IN (3,4,5) THEN 1 ELSE .126 END as state_uninsured,
            CASE WHEN p.race IN (3,4,5) THEN 1 ELSE .226 END as state_uninsured_underinsured
            """
        return(statements)
    
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
    
    def AggregateEligiblePopulation(self, geogAggregationType="individual", geogUnitColumnName="sp_hh_id",  geogAggregationTable=""):
        """
        We have two query statements because for aggregations we need group by the geographic unit and the geometry.
        The table aliases are b (aggregate) and c (individual)
        """
        
        theDict = {"geogColumnName": geogUnitColumnName}
        if not geogAggregationType == "individual":
            theDict["aggregationStatement"] = "inner join {} b on ST_Intersects(c.geom, b.geom)".format(geogAggregationTable) 
            query = """eligible_aggregation as
                (
                select b.{geogColumnName} as bound_id, ST_Transform(ST_Centroid(b.geom),26915) as geom,
                sum(c.expected_population) as expected_population
                from uninsured_population c {aggregationStatement}
                group by b.{geogColumnName}, b.geom
                )""".format(**theDict)
        else:
            theDict["aggregationStatement"] = ""
            query = """eligible_aggregation as
                (
                select c.{geogColumnName} as bound_id, st_transform(c.geom,26915) as geom,
                sum(c.expected_population) as expected_population
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
            
            theDict = {"geogColumnName": geogUnitColumnName, "geogTableName": geoAggregationTable }
            
            numeratorAggregator = """clients_aggregate as
            (
            SELECT b.{geogColumnName} as bound_id, count(c.client_id) as num_clients, ST_Transform(ST_Centroid(b.geom),26915) as geom
            FROM sage.breast_clients_2010_2015 c
            INNER JOIN {geogTableName} b on ST_Intersects(c.geom, b.geom)
            GROUP BY b.{geogColumnName}, b.geom
            ),""".format(**theDict)
        else:
            numeratorAggregator = """clients_aggregate as
            (
            SELECT c.gid as bound_id, 1 as num_clients, ST_Transform(ST_Centroid(c.geom),26915) as geom
            FROM sage.breast_clients_2010_2015 c
            INNER JOIN sage.mn_border b on ST_Intersects(c.geom, b.geom)
            ),"""
        
        query = """{} numerator as
            (
            SELECT b.gid, b.geom, sum(c.num_clients) as num_clients
            FROM buffer_definition b 
            LEFT JOIN clients_aggregate c on ST_DWithin( b.geom,  c.geom, b.min_buffer_distance)
            GROUP BY b.gid, b.geom
            ORDER BY b.gid
            )""".format(numeratorAggregator,  )
        
        return(query)
    
    def FilterRelativeRiskCalculation(self,):
        """
        """
        query = r"""SELECT d.gid, ST_AsText(d.geom) as geom, num_clients as num, expected_population*5 as denom, 
            num_clients/(expected_population*5)::float as ratio, num_geographic_features as num_features, bd.min_buffer_distance as buffer_size
            FROM denominator d INNER JOIN numerator n ON (n.gid = d.gid)  INNER JOIN buffer_definition bd  on (n.gid = bd.gid)"""
        
        return(query)
    
    def BreastCancerUtilizationCalc(self, aggregationType="individual", aggregationTable="", aggregationTableColumnName="", caseStatement=None, uninsuredField=None):
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
            
            
            sqlQueryList.append(self.CalculateUninsuredPoplation(caseStatement, uninsuredField))
            
            
            sqlQueryList.append(self.GridPartitioning(sqlDict))
            sqlQueryList.append(self.AggregateEligiblePopulation(aggregationType, aggregationTableColumnName, aggregationTable, ))
            sqlQueryList.append(self.BufferCalculation())
            sqlQueryList.append(self.CalculateDenominator())
            sqlQueryList.append(self.CalculateNumerator(aggregationTable, aggregationTableColumnName))
            sqlQueryList.append(self.FilterRelativeRiskCalculation())
            
            
            
    
            theQuery = self.PrettySQL(sqlQueryList, -1)
            #print(minGridID, maxGridID)
            
            maxGridID += 1000
            listOfQueries.append(theQuery)
        

        return(listOfQueries)

    def myFunction(self, inparam):
        """
        
        """
        print("hello", inparam)
    #        self.psqlConn = self.CreateConnection(self.psqlConnectionDict)
    #        self.psqlCur = self.psqlConn.cursor(cursor_factory=extras.DictCursor)
            
        
    
    def parallelAnalysis(self,numCores=10):
        """
        
        """
        pool = mp.Pool(numCores)
        print(numCores)
        try:
            results = pool.imap(self.myFunction, range(1,10) )
            pool.imap()
        except:
            print(mp.get_logger())
        
        pool.close()
        pool.join()
                