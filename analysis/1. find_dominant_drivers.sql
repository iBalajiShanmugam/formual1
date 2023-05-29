-- Databricks notebook source
select
  driver_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_poitns
from f1_presentation.calculated_race_results
group by driver_name
having total_races >= 50
order by avg_poitns desc

-- COMMAND ----------

select
  driver_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_poitns
from f1_presentation.calculated_race_results
where race_year between 2001 and 2010
group by driver_name
having total_races >= 50
order by avg_poitns desc

-- COMMAND ----------


