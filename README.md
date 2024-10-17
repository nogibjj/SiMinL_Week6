[![CI](https://github.com/nogibjj/SiMinL_Week6/actions/workflows/hello.yml/badge.svg)](https://github.com/nogibjj/SiMinL_Week6/actions/workflows/hello.yml)

# SiMinL_MiniProj6

# Requirements
Design a complex SQL query involving joins, aggregation, and sorting
Provide an explanation for what the query is doing and the expected results

# Purpose 
The goal of this project is to create an ETL-Query pipeline utilizing a cloud service like Databricks. This pipeline will involve tasks such as extracting data from FiveThirtyEight's public datasets, cleaning and transforming the data, then loading it into Databricks SQL Warehouse. Once the data is in place, complex queries like joining tables, aggregating data, and sorting results can be done. This will be accomplished by establishing a database connection to Databricks.

# Preparation
wait for container to be built and virtual environment to be activated with requirements.txt installed
make an .env file to store Databricks' connection 
extract: run make extract
transform and load: run make transform_load
query: run make query

# Sample Complex Query and Explanation
SELECT t2.Major, t1.FOD1P,
                t2.women/(t2.total) as womenshare
            FROM default.majorsDB t1
            JOIN default.womenstemDB t2 ON t1.Major = t2.Major
            ORDER BY womenshare DESC
            LIMIT 10
This query retrieves data from two tables, majorsDB and womenstemDB which contain information about different college majors and the gender distribution within those majors. An inner join is performed on the Major column. This query selects the name of the major, a field of study code and calculates the proportion of women in each major by dividing the number ofwomen by the total number of students in the major. The results are ordered in descending order based on the womenshare, effectively identifying the majors with the highest proportion of women. This provides insights into which fields women are most represented in.

<img width="691" alt="image" src="https://github.com/user-attachments/assets/b1396d78-8240-4bf3-ab7d-7620904fdd38">





