-- Databricks notebook source
create database if not exists f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create Circuits Table

-- COMMAND ----------

use f1_raw;

drop TABLE if exists f1_raw.circuits;
create table if not exists f1_raw.circuits(
  circuitId int,
  circuitRef string,
  name string,
  location string,
  country String,
  lat double,
  lng double,
  alt int,
  url string
)
using csv
options(path '/mnt/formula1dldataset/raw/circuits.csv', header true)

-- COMMAND ----------

select * from f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Races table

-- COMMAND ----------

use f1_raw;

drop TABLE if exists f1_raw.races;
create table if not exists f1_raw.races(
  raceId int,
  year int,
  round int,
  circuitId int,
  name string,
  date date,
  time string,
  url string
)
using csv
options(path '/mnt/formula1dldataset/raw/races.csv', header true)

-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Creating tabe for json

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### creat constructors tables
-- MAGIC 1. singe line json
-- MAGIC 2. simple structure

-- COMMAND ----------

use f1_raw;
Drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors(
  constructorId int,
  constructorRef string,
  name string,
  nationality string,
  url string
)
using json
options(path '/mnt/formula1dldataset/raw/constructors.json')

-- COMMAND ----------

select * from f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create driver table
-- MAGIC 1. single line JSON
-- MAGIC 2. complex structure

-- COMMAND ----------

use f1_raw;
Drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers(
  driverId int,
  driverRef string,
  number int,
  code string,
  name struct<forename: string, surname: string>,
  dob Date,
  nationality string,
  url string
)
using json
options(path '/mnt/formula1dldataset/raw/drivers.json')

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### creat results tables
-- MAGIC 1. singe line json
-- MAGIC 2. simple structure

-- COMMAND ----------

use f1_raw;
Drop table if exists f1_raw.results;
create table if not exists f1_raw.results(
  resultId int,
  raceId int,
  driverId int,
  constructorId int,
  number int,
  grid int,
  position int,
  positionText string,
  positionOrder int,
  points int,
  laps int,
  time string,
  milliseconds int,
  fastestLap int,
  fastestLapSpeed string,
  statusId string
)
using json
options(path '/mnt/formula1dldataset/raw/results.json')

-- COMMAND ----------

select * from f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### creat pit stops tables
-- MAGIC 1. multi line json
-- MAGIC 2. simple structure

-- COMMAND ----------

use f1_raw;
Drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops(
  driverId int,
  duration string,
  lap int,
  milliseconds int,
  raceId int,
  stop int,
  time string
)
using json
options(path '/mnt/formula1dldataset/raw/pit_stops.json', multiLine true)

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create tables for ist of file

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### create Lap Times tables
-- MAGIC 1. CSV file
-- MAGIC 2. Multiple files

-- COMMAND ----------

use f1_raw;
Drop table if exists f1_raw.lap_times;
create table if not exists f1_raw.lap_times(
  raceId int,
  driverId int,
  lap int,
  position int,
  time string,
  milliseconds int
)
using csv
options(path '/mnt/formula1dldataset/raw/lap_times')

-- COMMAND ----------

select * from f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Qualifying tables
-- MAGIC 1. JSON file
-- MAGIC 2. MultiLine JSON
-- MAGIC 2. Multiple files

-- COMMAND ----------

use f1_raw;
Drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying(
  qualifyId int,
  raceId int,
  driverId int,
  constructorsId int,
  number int,
  position int,
  q1 string,
  q2 string,
  q3 string
)
using json
options(path '/mnt/formula1dldataset/raw/qualifying', multiLine true)

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------


