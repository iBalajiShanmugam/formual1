-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color: Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Teams </h1> """
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace temp view v_dominant_teams
as
select
  team_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_poitns,
  rank() over (order by avg(calculated_points) desc ) team_rank
from f1_presentation.calculated_race_results
group by team_name
having total_races >= 50
order by avg_poitns desc

-- COMMAND ----------

select
  race_year,
  team_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_poitns
from f1_presentation.calculated_race_results
where team_name in (select team_name from v_dominant_teams where team_rank <=5)
group by team_name, race_year
order by race_year, avg_poitns desc

-- COMMAND ----------

select
  race_year,
  team_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_poitns
from f1_presentation.calculated_race_results
where team_name in (select team_name from v_dominant_teams where team_rank <=5)
group by team_name, race_year
order by race_year, avg_poitns desc


-- COMMAND ----------

