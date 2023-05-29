-- Databricks notebook source
use f1_processed;

-- COMMAND ----------

drop table if exists f1_presentation.calculated_race_results;
create table f1_presentation.calculated_race_results
using parquet
as
select
    races.race_year,
    constructors.name as team_name,
    drivers.name as driver_name,
    results.position,
    results.points,
    11 - results.position as calculated_points
  from f1_processed.results
  join f1_processed.drivers on (results.driver_id = drivers.driver_id)
  join f1_processed.constructors on (results.constructor_id = constructors.constructor_id)
  join f1_processed.races on (races.race_id = results.race_id)
where 
  results.position <=10

-- COMMAND ----------


