# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------

spark.conf.set("pipelines.incompatibleViewCheck.enabled", "false")
catalog = "main"
schema = "sdp_ut_dev"
volume_name = "raw_data"
volume_folder =  f"/Volumes/{catalog}/{schema}/{volume_name}"

# COMMAND ----------

users_data = [
    {"id":1,"email":"joneschristina@example.org","creation_date":"11-28-2021 12:08:46","last_activity_date":"08-20-2021 08:24:44","firstname":"Randall","lastname":"Espinoza","address":"71571 Jennifer Creek - East John, CO 81653","city":"Port Nicholas","last_ip":"22.207.225.77","postcode":"62389"},
    {"id":4,"email":"christybautista@example.net","creation_date":"06-30-2022 22:51:30","last_activity_date":"08-22-2021 17:25:06","firstname":"Jose","lastname":"Bell","address":"865 Young Crest - Lake Adriennebury, VA 67749","city":"Brownstad","last_ip":"159.111.101.250","postcode":"52432"},
    {"id":0,"email":"amccormick@example.com","creation_date":"10-21-2021 02:37:38","last_activity_date":"07-22-2021 15:06:48","firstname":"Dylan","lastname":"Barber","address":"7995 Ronald Flat Suite 597 - Williefurt, AL 37894","city":"Port Steven","last_ip":"173.88.213.168","postcode":"58368"},
    {"id":3,"email":"jenniferbennett@example.org","creation_date":"07-06-2022 12:27:24","last_activity_date":"01-09-2022 15:04:45","firstname":"Phillip","lastname":"Morgan","address":"523 Garza Crossroad - New Maryview, OK 92301","city":"Julieshire","last_ip":"170.233.120.199","postcode":"34528"},
    {"id":2,"email":"alexis25@example.org","creation_date":"09-10-2021 02:31:37","last_activity_date":"01-11-2022 20:39:01","firstname":"Gregory","lastname":"Crane","address":"068 Shawn Port - West Jessica, KS 84864","city":"South Tonya","last_ip":"192.220.63.96","postcode":"88033"},
    {"email":"davidporter@example.com","creation_date":"05-28-2022 09:54:50","last_activity_date":"12-18-2021 21:48:48","firstname":"Jeremy","lastname":"Knight","address":"06183 Acevedo Bypass - Petermouth, ME 34177","city":"West Brianburgh","last_ip":"53.240.159.208","postcode":"73380"},
    {"id":"invalid ID","email":"margaret84@example.com","creation_date":"12-20-2021 19:57:28","last_activity_date":"07-27-2021 09:39:28","firstname":"Angela","lastname":"Adams","address":"098 Daniel Ferry Suite 565 - South Andrea, ND 36326","city":"New Mariafort","last_ip":"7.176.250.65","postcode":"21300"}
]

@dp.materialized_view(comment="Raw user data - Dev")
def raw_user_data():
  return (spark.createDataFrame(users_data))

# COMMAND ----------

spend_data = [
    {"id":"3","age":47,"annual_income":858.9,"spending_core":99.4},
    {"id":"1","age":47,"annual_income":861.9,"spending_core":48.1},
    {"id":"2","age":97,"annual_income":486.4,"spending_core":880.8},
    {"id":"4","age":None,"annual_income":283.8,"spending_core":117.8},
    {"id":None,"age":95,"annual_income":847.5,"spending_core":840.9},
    {"id":"invalid_id","age":1,"annual_income":514.5,"spending_core":284.5}
]

@dp.materialized_view(comment="Raw spend data - Dev")
def raw_spend_data():
  return (spark.createDataFrame(spend_data))