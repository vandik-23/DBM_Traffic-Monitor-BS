# Project: Traffic Monitor Basel-Stadt (MySQL Database)

## Decription:
The aim of this project is to create relational database from existing and publicly available footfall & bicycle traffic and weather data of Basel City.
The project follows an ETL (Extract-Transform-Load) process. 

## Data Sources:

[Traffic Numbers Bicycles and Pedestrians (Basel city)](https://data-bs.ch/mobilitaet/converted_Velo_Fuss_Count.csv)
Results of the measurements of the permanent counting stations and short-term counting stations for bicycle and pedestrian traffic. 
The counting data for pedestrian traffic are adjusted monthly by applying a correction function and published afterwards.

[Meteo Data Northern Western Switzerland](https://meteodaten-nordwest.ch/s/datenexport)
Data contains measurements in 30 Minutes frequency at one measuring station in Basel City. 
The measurements contain the temperature (°C), precipitation (mm), windspeed (m/s), wind direction (°), global radiation (W/m²).

## Database Architecture:
![schema](https://github.com/vandik-23/DBM_Traffic-Monitor-BS/blob/main/DB_Schema.png)
