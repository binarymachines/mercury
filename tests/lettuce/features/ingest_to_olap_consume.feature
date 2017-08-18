Feature: pull source data from file, transform to CRS, and write to star schema

  @sst
  Scenario: raw record extract, transform to Core Record Schema and then to OLAP
    given we load data from the source file to the source topic
    when we read and transform the records
    and load the core records
    and we consume the core records from staging into OLAP
    then we get the same number of core records in the target topic
    and we get the same number of records in the fact table of the schema


    
    
 