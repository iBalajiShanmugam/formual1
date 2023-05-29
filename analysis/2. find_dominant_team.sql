-- Databricks notebook source
select
  team_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_poitns
from f1_presentation.calculated_race_results
group by team_name
having total_races >= 100
order by avg_poitns desc

-- COMMAND ----------

select
  team_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_poitns
from f1_presentation.calculated_race_results
where race_year between 2011 and 2020
group by team_name
having total_races >= 100
order by avg_poitns desc

-- COMMAND ----------


