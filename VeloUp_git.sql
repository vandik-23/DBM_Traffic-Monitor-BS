CREATE DATABASE veloup;
DROP TABLE WEATHER;
DROP TABLE TRAFFIC;

# to be updated from FLOAT to DECIMAL: Temp, Precip, Windspeed
# to be updated from INTEGER to SMALLINT: Wind dir, GlobalRadiation

USE veloup;

CREATE TABLE WEATHER (
DateTime_Weather DATETIME PRIMARY KEY NOT NULL,
Temperature DECIMAL (6,2) NULL,
Precipation DECIMAL(6,2) NULL, 
WindSpeed DECIMAL (6,2) NULL, 
WindDirection SMALLINT NULL,
GlobalRadiation SMALLINT NULL);

# Renaming columns:
ALTER TABLE WEATHER
RENAME COLUMN Windspped TO Windspeed;

# Checking and modifying data typ in table weather:
SELECT * FROM WEATHER;
#to show data typ and Zeros of a table.
DESCRIBE WEATHER; 
# Modify character typ
ALTER TABLE WEATHER
MODIFY DateTime_Weather DATETIME NOT NULL,
MODIFY Temperature DECIMAL(6,2) NULL,
MODIFY Precipation DECIMAL(6,2) NULL,
MODIFY WindSpeed DECIMAL(6,2) NULL,
MODIFY WindDirection SMALLINT NULL,
MODIFY GlobalRadiation SMALLINT NULL;

DROP TABLE TRAFFIC;

CREATE TABLE TRAFFIC (
TrafficID INTEGER PRIMARY KEY AUTO_INCREMENT,
DateTime_Weather DATETIME NOT NULL,
LocationID VARCHAR(10) NOT NULL, 
TrafficTypID INTEGER NOT NULL,
DateTimeTo DATETIME NOT NULL,
TotalCount INTEGER,
CONSTRAINT DateTime_Weather_FK
FOREIGN KEY (DateTime_Weather) REFERENCES WEATHER(DateTime_Weather),
CONSTRAINT LocationID_FK
FOREIGN KEY (LocationID) REFERENCES LOCATION(LocationID),
CONSTRAINT TrafficTypID_FK
FOREIGN KEY (TrafficTypID) REFERENCES TYP(TrafficTypID),
CONSTRAINT DateTimeTo_FK
FOREIGN KEY (DateTimeTo) REFERENCES TRAFFIC_TIME(DateTimeTo));

# Checking and modifying TRAFFIC
DESCRIBE TRAFFIC;
SELECT * FROM TRAFFIC;

# Removing foreign keys
ALTER TABLE TRAFFIC DROP  FOREIGN KEY DateTime_Weather_FK;
ALTER TABLE TRAFFIC DROP  FOREIGN KEY LocationID_FK;
ALTER TABLE TRAFFIC DROP  FOREIGN KEY TrafficTypID_FK;
ALTER TABLE TRAFFIC DROP  FOREIGN KEY TimeID_FK;

# Adding back Foreign keys:
ALTER TABLE TRAFFIC
ADD CONSTRAINT DateTime_Weather_FK
FOREIGN KEY (DateTime_Weather) REFERENCES WEATHER(DateTime_Weather);

ALTER TABLE TRAFFIC
ADD CONSTRAINT LocationID_FK
FOREIGN KEY (LocationID) REFERENCES LOCATION(LocationID);

ALTER TABLE TRAFFIC
ADD CONSTRAINT TrafficTypID_FK
FOREIGN KEY (TrafficTypID) REFERENCES TYP(TrafficTypID);

# Modifying Table Traffic:
ALTER TABLE TRAFFIC
MODIFY DateTime_Weather DATETIME NOT NULL,
MODIFY LocationID VARCHAR(10) NOT NULL,
MODIFY TrafficTypID INTEGER NOT NULL,
MODIFY TotalCount INTEGER NULL;

ALTER TABLE TRAFFIC
MODIFY TrafficTypID BIGINT NOT NULL;

DROP TABLE LOCATION;

CREATE TABLE LOCATION (
LocationID VARCHAR(10) PRIMARY KEY NOT NULL,
SiteCode VARCHAR(250) NOT NULL,
SiteName VARCHAR(250) NULL,
DirectionName VARCHAR(250) NULL,
LaneCode DECIMAL(6,2) NOT NULL,
LaneName VARCHAR(250) NULL,
SiteID INTEGER NULL);

# Checking and modifying LOCATION
DESCRIBE LOCATION;
SELECT * FROM LOCATION;
ALTER TABLE LOCATION DROP PRIMARY KEY;

# Modifying Table Location:
ALTER TABLE LOCATION
MODIFY LocationID VARCHAR(10) NOT NULL,
MODIFY SiteCode VARCHAR(250) NOT NULL,
MODIFY SiteName VARCHAR(250) NULL,
MODIFY DirectionName VARCHAR(250) NULL,
MODIFY LaneCode DECIMAL(6,2) NOT NULL,
MODIFY LaneName VARCHAR(250) NULL;


DROP TABLE TRAFFIC_TIME;

CREATE TABLE TRAFFIC_TIME (
DateTimeFrom DATETIME NULL,
DateTimeTo DATETIME PRIMARY KEY NOT NULL,
YearValue SMALLINT NULL,
MonthValue TINYINT NULL,
DayValue TINYINT NULL,
Weekday TINYINT NULL);

# Checking and modifying TRAFFIC_TIME:
DESCRIBE TRAFFIC_TIME;
SELECT * FROM TRAFFIC_TIME;

ALTER TABLE TRAFFIC_TIME 
MODIFY YearValue SMALLINT NULL,
MODIFY MonthValue TINYINT NULL,
MODIFY DayValue TINYINT NULL,
MODIFY Weekday TINYINT NULL;

# Adding check constraints to table:
ALTER TABLE TRAFFIC_TIME
ADD CONSTRAINT CK_Month
CHECK (MonthValue >= 0 AND MonthValue <= 12);

ALTER TABLE TRAFFIC_TIME
ADD CONSTRAINT CK_Year
CHECK (YearValue > 1900);

ALTER TABLE TRAFFIC_TIME
ADD CONSTRAINT CK_Day
CHECK (DayValue >= 0 AND DayValue <= 31);

ALTER TABLE TRAFFIC_TIME
ADD CONSTRAINT CK_WeekDay
CHECK (Weekday >= 0 AND Weekday <= 7);

DROP TABLE TYP;
TRUNCATE TABLE TYP;

CREATE TABLE TYP (
TrafficTypID SERIAL PRIMARY KEY NOT NULL,
TrafficTyp VARCHAR(100) NOT NULL);

ALTER TABLE TYP
RENAME COLUMN TrafficType TO TrafficTyp;

# Checking and modifying TRAFFIC
DESCRIBE TYP;
SELECT * FROM TYP;

ALTER TABLE TYP
MODIFY TrafficTyp VARCHAR(100) NOT NULL;

# Loading weather data into MySQL:

# Prepared server to load local files
SET GLOBAL local_infile = true ;

LOAD DATA LOCAL INFILE '~/Project/meteodaten_(3).csv'
INTO TABLE  weather
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
IGNORE 3 LINES
(@DateTime_Weather, Temperature, Precipation, Windspeed, WindDirection, GlobalRadiation)
SET DateTime_Weather = STR_TO_DATE(@DateTime_Weather, '%Y-%m-%d %H:%i:%s');

# Checking if the upload has taken place:
SELECT * FROM veloup.weather;

# Empty a table: 
TRUNCATE TABLE weather;

# turning out the Foreign Key constraints, given it makes the load very slow:
ALTER TABLE traffic DROP CONSTRAINT DateTime_Weather_FK;
ALTER TABLE traffic DROP CONSTRAINT LocationID_FK;
ALTER TABLE traffic DROP CONSTRAINT TimeID_FK;
ALTER TABLE traffic DROP CONSTRAINT TrafficTypID_FK;
ALTER TABLE traffic DROP PRIMARY KEY;

# creating a traffic source file: 
CREATE TABLE TRAFFIC_Source (
SiteCode VARCHAR(250),
SiteName VARCHAR(250),
DirectionName VARCHAR(250),
LaneCode DECIMAL (6,2),
LaneName VARCHAR(250),
#CountDate DATE,
#TimeFrom TIME,
TimeTo DATETIME,
#ValuesApproved BINARY,
#ValuesEdited BINARY,
TrafficTyp VARCHAR(100),
TotalCount INTEGER,
DateTimeFrom DATETIME,
DateTimeTo DATETIME,
YearValue SMALLINT,
MonthValue TINYINT,
DayValue TINYINT,
Weekday TINYINT,
#HourFrom TIME,
#DayOfYear SMALLINT,
SiteID INTEGER);

DROP TABLE TRAFFIC_Source;
# Empty a table: 
TRUNCATE TABLE TRAFFIC_Source;

LOAD DATA LOCAL INFILE '~/Project/converted_Velo_Fuss_Count_(1).csv'
INTO TABLE  TRAFFIC_source
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(SiteCode, SiteName, DirectionName, LaneCode, LaneName, @dummy, @dummy, @TimeTo, @dummy, @dummy, TrafficType, TotalCount, 
@DateTimeFrom, @DateTimeTo, YearValue, MonthValue, DayValue, Weekday, @dummy, @dummy, SiteID)
SET TimeTo = STR_TO_DATE(@TimeTo, '%H:%i'),
DateTimeFrom = STR_TO_DATE(@DateTimeFrom, '%Y-%m-%d %H:%i:%s'),
DateTimeTo = STR_TO_DATE(@DateTimeTo, '%Y-%m-%d %H:%i:%s');

# Checking if the upload has taken place:
SELECT * FROM veloup.TRAFFIC_source;

# Empty a table: 
TRUNCATE TABLE TRAFFIC_source;

#select all datetime where our two databases do not overlap:
SELECT * FROM TRAFFIC_source WHERE DateTimeTo NOT IN
(SELECT DateTime_Weather FROM weather);

SELECT * FROM weather WHERE DateTime_Weather NOT IN
(SELECT DateTimeTo FROM TRAFFIC_source);

# delete all datetime where our two databases do not overlap:
DELETE FROM TRAFFIC_source WHERE DateTimeTo NOT IN
(SELECT DateTime_Weather FROM weather);

DELETE FROM weather WHERE DateTime_Weather NOT IN
(SELECT DateTimeTo FROM TRAFFIC_source);

#checking how many years/days we have overlapping in our databases:
SELECT DISTINCT YEAR(DateTime_Weather)
FROM weather;

###########################################################3

# normalizing TYP:
SELECT DISTINCT TrafficTyp
FROM traffic_source;

SELECT * FROM traffic_source
WHERE TrafficTyp <> 'Velo' AND TrafficTyp <> 'Fussgänger';

UPDATE traffic_source
SET TrafficTyp = 'unbekannt'
WHERE TrafficTyp = '';

INSERT INTO typ(TrafficTypID)
SELECT DISTINCT TrafficTyp
FROM traffic_source
WHERE TrafficTyp IS NOT NULL;

# Checking if the upload has taken place:
SELECT * FROM veloup.typ;

#####################################################################3

# normalizing LOCATION:
ALTER TABLE traffic_source
ADD LocationID FLOAT; 

UPDATE traffic_source
SET LaneCode = 1 WHERE LaneName LIKE '%1%';

UPDATE traffic_source
SET LaneCode = 2 WHERE LaneName LIKE '%2%';

UPDATE traffic_source
SET LocationID = SiteID + (LaneCode/10);

UPDATE traffic_source
SET LocationID = LocationID + 0.01 WHERE SiteCode LIKE '%a%';

UPDATE traffic_source
SET LocationID = LocationID + 0.001 WHERE SiteCode LIKE '%aa%';

UPDATE traffic_source
SET LocationID = LocationID + 0.03 WHERE DirectionName LIKE '%steg%';

UPDATE traffic_source
SET LocationID = LocationID + 0.03 WHERE DirectionName LIKE '%rheinweg%';

ALTER TABLE traffic_source
MODIFY LocationID VARCHAR(10) NOT NULL;


SELECT * FROM traffic_source
WHERE SiteCode = '804aa';

#First checking location code:
SELECT DISTINCT (LocationID)
FROM traffic_source;

SELECT * FROM traffic_source
WHERE LocationID IS NOT NULL;

SELECT * FROM traffic_source
WHERE DateTimeTo = '2015-02-01 21:00:00';

SELECT * FROM traffic_source
WHERE LocationID IS NULL;

DELETE FROM traffic_source
WHERE LocationID IS NULL;

SELECT * FROM traffic_source;

# Empty a table: 
TRUNCATE TABLE location;

ALTER TABLE location DROP PRIMARY KEY;

INSERT INTO location(LocationID, SiteCode, SiteName, DirectionName, LaneCode, LaneName, SiteID)
SELECT DISTINCT LocationID, SiteCode, SiteName, DirectionName, LaneCode, LaneName, SiteID
FROM traffic_source;

ALTER TABLE location ADD CONSTRAINT
PRIMARY KEY (LocationID);

SELECT * FROM location
WHERE LocationID IS NULL;

DELETE FROM location
WHERE LocationID IS NULL;

SELECT * FROM location;

SELECT * FROM location
WHERE DirectionName LIKE '%rheinweg%';

#######################################################################################

# Normalizing TIME
ALTER TABLE traffic_time DROP PRIMARY KEY;

ALTER TABLE traffic_time
RENAME COLUMN MonthVanue TO MonthValue;

INSERT INTO traffic_time(DateTimeFrom, DateTimeTo, YearValue, MonthValue, DayValue, Weekday)
SELECT DISTINCT DateTimeFrom, DateTimeTo, YearValue, MonthValue, DayValue, Weekday
FROM traffic_source;

ALTER TABLE traffic_time ADD CONSTRAINT
PRIMARY KEY (DateTimeTo);

SELECT * FROM traffic_time;


# Empty a table: 
TRUNCATE TABLE traffic_time;

SELECT DISTINCT DateTimeTo
FROM traffic_source;

################################################################################

# Loading & connectiong the TRAFFIC table

# First we remove all Primary and foreign key constraints before the loading:
ALTER TABLE traffic DROP PRIMARY KEY;
ALTER TABLE traffic DROP CONSTRAINT DateTime_Weather_FK;
ALTER TABLE traffic DROP CONSTRAINT LocationID_FK;
ALTER TABLE traffic DROP CONSTRAINT TrafficTypID_FK;
ALTER TABLE traffic DROP CONSTRAINT DateTimeTo_FK;

DROP TABLE TRAFFIC;
DESCRIBE typ;


# recreated the traffic table for 2 reasons:
# - change typ of TrafficTypID to bignit unsigned,
# - change name TimeID to DateTimeTo column

CREATE TABLE TRAFFIC (
TrafficID SERIAL PRIMARY KEY,
DateTime_Weather DATETIME,
LocationID FLOAT,
TrafficTypID bigint unsigned,
DateTimeTo DATETIME,
TotalCount INTEGER,
CONSTRAINT DateTime_Weather_FK
FOREIGN KEY (DateTime_Weather) REFERENCES WEATHER(DateTime_Weather),
CONSTRAINT LocationID_FK
FOREIGN KEY (LocationID) REFERENCES LOCATION(LocationID),
CONSTRAINT TrafficTypID_FK
FOREIGN KEY (TrafficTypID) REFERENCES TYP(TrafficTypID),
CONSTRAINT DateTimeTo_FK
FOREIGN KEY (DateTimeTo) REFERENCES TRAFFIC_TIME(DateTimeTo));

# Loading data into traffic from traffic source table:

INSERT INTO traffic(LocationID, DateTimeTo, TotalCount, TrafficTypID)
SELECT LocationID, DateTimeTo, TotalCount, TrafficTypID
FROM traffic_source;

# Loading data from weather + creating the connection: 
UPDATE  traffic
SET traffic.DateTime_Weather = (
SELECT DateTime_Weather
FROM weather
WHERE traffic.DateTimeTo = weather.DateTime_Weather);

SELECT * FROM traffic;

# Added a column with TrafficTypID to traffic_source
ALTER TABLE typ
RENAME COLUMN TrafficTypName TO TrafficTyp;

ALTER TABLE traffic_source
ADD TrafficTypID INTEGER;

UPDATE  traffic_source
SET traffic_source.TrafficTypID = (
SELECT TrafficTypID
FROM typ
WHERE traffic_source.TrafficType = typ.TrafficType);

#checking tables - results:
SELECT * FROM traffic
WHERE TrafficTypID = 3;

# Empty a table: 
TRUNCATE TABLE traffic;

SELECT MAX(TrafficID)

FROM traffic;

# Query 1: select locations, where bycicle or pedestrian numbers are high.
SELECT * FROM TRAFFIC;
SELECT * FROM TYP;
SELECT * FROM LOCATION;
SELECT * FROM traffic_has_typ;

# Empty a table: 
TRUNCATE TABLE traffic_fequency;
DROP TABLE traffic_fequency;

##########################################################################

# Query 1: select locations, where bycicle or pedestrian numbers are high using TRAFFIC table.
SELECT LocationID, TrafficTypID, SUM(TotalCount) as Total
FROM TRAFFIC
WHERE TrafficTypID = 1 OR TrafficTypID = 2
GROUP BY LocationID, TrafficTypID
ORDER BY Total desc
LIMIT 10;

###################################################################################

# Show the 10 locations where the number of bikers or pedestrians is highest overall:
SELECT LocationID, TrafficTypID, SUM(TotalCount) as Total
FROM TRAFFIC
WHERE TrafficTypID = 1 OR TrafficTypID = 2
GROUP BY LocationID, TrafficTypID
ORDER BY Total desc
LIMIT 10;

# Show the 10 locations where the number of bikers is highest overall:
SELECT LocationID, 
SUM(TotalCount) as Total
FROM TRAFFIC
WHERE TrafficTypID = 1
GROUP BY LocationID
ORDER BY Total desc
LIMIT 10;

# Show the 10 locations where the number of Pedestrian is highest overall:
SELECT LocationID, 
SUM(TotalCount) as Total
FROM TRAFFIC
WHERE TrafficTypID = 2
GROUP BY LocationID
ORDER BY Total desc
LIMIT 10;

# Show most busy locations by Typ, Location ID the highest number of bikers descending orders:
SELECT
ty.TrafficTyp, 
tr.LocationID, 
SUM(tr.TotalCount) as Total 
FROM TRAFFIC AS tr
INNER JOIN TYP as ty
ON tr.TrafficTypID = ty.TrafficTypID
WHERE ty.TrafficTyp = 'Velo'
GROUP BY LocationID
ORDER BY Total desc;

# Show most busy locations by Typ, LocationName, Direction, Lane the highest number of bikers descending orders:
SELECT
lo.SiteName,
lo.DirectionName,
lo.LaneName,
ty.TrafficTyp, 
SUM(tr.TotalCount) as Total 
FROM TRAFFIC AS tr
INNER JOIN TYP as ty
ON tr.TrafficTypID = ty.TrafficTypID
LEFT JOIN LOCATION as lo
ON tr.LocationID = lo.LocationID
WHERE ty.TrafficTyp = 'Velo'
GROUP BY lo.LocationID
ORDER BY Total desc;

# Show most busy locations by Typ, LocationName, Direction, Lane, Time the highest number of bikers descending orders:
SELECT 
lo.SiteName,
lo.DirectionName,
lo.LaneName,
ti.YearValue,
ty.TrafficTyp, 
SUM(tr.TotalCount) as Total,
ti.YearValue,
ti.MonthValue,
ti.Weekday
FROM TRAFFIC AS tr
INNER JOIN TYP as ty
ON tr.TrafficTypID = ty.TrafficTypID
LEFT JOIN LOCATION as lo
ON tr.LocationID = lo.LocationID
LEFT JOIN TRAFFIC_TIME as ti
ON tr.DateTimeTo = ti.DateTimeTo
WHERE (ty.TrafficTyp = 'Velo' AND  ti.YearValue= 2015)
GROUP BY tr.LocationID, ti.YearValue, ti.MonthValue, ti.Weekday
ORDER BY Total desc;

SELECT MAX(YearValue) FROM TRAFFIC_TIME;
SELECT MIN(YearValue) FROM TRAFFIC_TIME;

DROP VIEW traffic_frequency;

######################################################################################

# Show most frequented locations by SiteName, Year, Month and Weekday 
# with the highest number of bikers in descending orders:

#CREATE VIEW traffic_frequency AS
SELECT
lo.SiteName,
ty.TrafficTyp, 
SUM(tr.TotalCount) as Total,
ti.YearValue,
ti.MonthValue,
ti.Weekday
FROM TRAFFIC AS tr
INNER JOIN TYP as ty
ON tr.TrafficTypID = ty.TrafficTypID
LEFT JOIN LOCATION as lo
ON tr.LocationID = lo.LocationID
LEFT JOIN TRAFFIC_TIME as ti
ON tr.DateTimeTo = ti.DateTimeTo
WHERE ty.TrafficTyp IS NOT NULL
GROUP BY lo.SiteName, ti.YearValue, ti.MonthValue, ti.Weekday, ty.TrafficTyp
ORDER BY Total desc;

SELECT * FROM traffic_frequency;

######################################################################################

# Show the execution plan for Query 1:
CREATE VIEW traffic_frequency AS
SELECT
lo.SiteName,
ty.TrafficTyp, 
SUM(tr.TotalCount) as Total,
ti.YearValue,
ti.MonthValue,
ti.Weekday
FROM TRAFFIC AS tr
INNER JOIN TYP as ty
ON tr.TrafficTypID = ty.TrafficTypID
LEFT JOIN LOCATION as lo
ON tr.LocationID = lo.LocationID
LEFT JOIN TRAFFIC_TIME as ti
ON tr.DateTimeTo = ti.DateTimeTo
WHERE ty.TrafficTyp IS NOT NULL
GROUP BY lo.SiteName, ti.YearValue, ti.MonthValue, ti.Weekday, ty.TrafficTyp
ORDER BY Total desc;
 
SELECT
SiteName,
TrafficTyp, 
Total,
YearValue,
MonthValue,
Weekday
FROM traffic_frequency
WHERE ty.TrafficTyp ='Velo'
GROUP BY lo.SiteName, ti.YearValue, ti.MonthValue, ti.Weekday, ty.TrafficTyp
ORDER BY Total desc;

######################################################################################
TRUNCATE TABLE traffic_typ1;
DROP TABLE traffic_typ1;

# Create materialized table:
CREATE TABLE traffic_typ AS
SELECT 
LocationID,
TrafficTyp,
tr.TrafficTypID,
DateTimeTo, 
TotalCount
FROM TRAFFIC tr, TYP ty
WHERE ty.TrafficTyp IS NOT NULL AND tr.TrafficTypID = ty.TrafficTypID;

# Create materialized table:
CREATE TABLE traffic_typ1 AS
SELECT 
LocationID,
TrafficTyp,
tr.TrafficTypID,
DateTimeTo, 
TotalCount
FROM TRAFFIC tr, TYP ty
WHERE ty.TrafficTyp IS NOT NULL AND tr.TrafficTypID = ty.TrafficTypID
AND RAND() <0.25;

CREATE INDEX IX_TrafficTypID ON traffic_typ1(TrafficTypID);
CREATE INDEX IX_TrafficTyp ON traffic_typ1(TrafficTyp);
CREATE INDEX IX_LocationID ON traffic_typ1(LocationID);
CREATE INDEX IX_DateTimeTo ON traffic_typ1(DateTimeTo);


SELECT
lo.SiteName,
tr.TrafficTyp, 
SUM(tr.TotalCount) as Total,
ti.YearValue,
ti.MonthValue,
ti.Weekday
FROM traffic_typ1 AS tr
LEFT JOIN LOCATION as lo
ON tr.LocationID = lo.LocationID
LEFT JOIN TRAFFIC_TIME as ti
ON tr.DateTimeTo = ti.DateTimeTo
WHERE tr.TrafficTyp IS NOT NULL
GROUP BY lo.SiteName, ti.YearValue, ti.MonthValue, ti.Weekday, tr.TrafficTyp
ORDER BY Total desc
LIMIT 10;

SELECT * FROM traffic_frequency;

DROP INDEX IX_TrafficTypID_ty ON TYP;
DROP INDEX IX_TrafficTypID_tr ON TRAFFIC;
######################################################################################

SELECT * FROM WEATHER;
SELECT MAX(WindSpeed) FROM WEATHER;
SELECT AVG(WindSpeed) FROM WEATHER;
SELECT MIN(WindSpeed) FROM WEATHER;

# Query 2: Traffic & Weather

#CREATE VIEW snow_ice_frequency AS
#CREATE VIEW wind_frequency AS
#CREATE VIEW rain_frequency AS
CREATE VIEW sun_frequency AS
SELECT
lo.SiteName,
ty.TrafficTyp,
AVG(we.Temperature) as avg_temp,
AVG(we.Precipation) as avg_precip,
MAX(we.WindSpeed) as max_windsp,
AVG(we.GlobalRadiation) as avg_sun,
SUM(tr.TotalCount) as Total,
ti.YearValue,
ti.MonthValue,
ti.Weekday
FROM TRAFFIC AS tr
INNER JOIN TYP as ty
ON tr.TrafficTypID = ty.TrafficTypID
LEFT JOIN LOCATION as lo
ON tr.LocationID = lo.LocationID
LEFT JOIN TRAFFIC_TIME as ti
ON tr.DateTimeTo = ti.DateTimeTo
LEFT JOIN WEATHER as we
ON tr.DateTime_Weather = we.DateTime_Weather
WHERE ty.TrafficTyp IS NOT NULL
GROUP BY lo.SiteName, ti.YearValue, ti.MonthValue, ti.Weekday, ty.TrafficTyp
# Option A:
#HAVING (avg_temp <= 0 AND avg_precip > 0)
# Option B:
#HAVING max_windsp >= 8.5 
# Option C:
#HAVING avg_precip > 0
# Option D:
HAVING (avg_sun >= 150 AND avg_temp >= 20)
ORDER BY Total desc;

TRUNCATE TABLE snow_ice_frequency;
DROP TABLE snow_ice_frequency;
######################################################################################

# Create materialized table 2:
CREATE TABLE traffic_typ1 AS
SELECT 
LocationID,
TrafficTyp,
tr.TrafficTypID,
DateTimeTo,
DateTime_Weather, 
TotalCount
FROM TRAFFIC tr, TYP ty
WHERE ty.TrafficTyp IS NOT NULL AND tr.TrafficTypID = ty.TrafficTypID
AND RAND() <0.25;

CREATE INDEX IX_TrafficTypID ON traffic_typ1(TrafficTypID);
CREATE INDEX IX_TrafficTyp ON traffic_typ1(TrafficTyp);
CREATE INDEX IX_LocationID ON traffic_typ1(LocationID);
CREATE INDEX IX_DateTimeTo ON traffic_typ1(DateTimeTo);
CREATE INDEX IX_DateTime_Weather ON traffic_typ1(DateTime_Weather);

# Optimization Query 2:

SELECT
lo.SiteName,
tr.TrafficTyp,
AVG(we.Temperature) as avg_temp,
AVG(we.Precipation) as avg_precip,
MAX(we.WindSpeed) as max_windsp,
AVG(we.GlobalRadiation) as avg_sun,
SUM(tr.TotalCount) as Total,
ti.YearValue,
ti.MonthValue,
ti.Weekday
FROM traffic_typ1 AS tr
LEFT JOIN LOCATION as lo
ON tr.LocationID = lo.LocationID
LEFT JOIN TRAFFIC_TIME as ti
ON tr.DateTimeTo = ti.DateTimeTo
LEFT JOIN WEATHER as we
ON tr.DateTime_Weather = we.DateTime_Weather
WHERE tr.TrafficTyp IS NOT NULL
GROUP BY lo.SiteName, ti.YearValue, ti.MonthValue, ti.Weekday, tr.TrafficTyp
# Option A:
HAVING (avg_temp <= 0 AND avg_precip > 0)
# Option B:
#HAVING max_windsp >= 8.5 
# Option C:
#HAVING avg_precip > 0
ORDER BY Total desc
LIMIT 10;

######################################################################################
#7. Sicherheit
##############################################

#Code-Beispiel 10 Erstellung und Zuweisung einer Rolle einen Benutzer mit eingeschränkten Leserechten

CREATE DATABASE Visualization2;

CREATE OR REPLACE VIEW
Visualization2.traffic AS
SELECT * FROM veloup.traffic;

CREATE OR REPLACE VIEW
Visualization2.location AS
SELECT * FROM veloup.location;

CREATE OR REPLACE VIEW
Visualization2.traffic_time AS
SELECT * FROM veloup.traffic_time;

CREATE OR REPLACE VIEW
Visualization2.typ AS
SELECT * FROM veloup.typ;

CREATE OR REPLACE VIEW
Visualization2.weather AS
SELECT * FROM veloup.weather;

CREATE OR REPLACE VIEW
Visualization2.snow_ice_frequency AS
SELECT * FROM veloup.snow_ice_frequency;

CREATE OR REPLACE VIEW
Visualization2.rain_frequency AS
SELECT * FROM veloup.rain_frequency;

CREATE OR REPLACE VIEW
Visualization2.traffic_frequency AS
SELECT * FROM veloup.traffic_frequency;

CREATE OR REPLACE VIEW
Visualization2.wind_frequency AS
SELECT * FROM veloup.wind_frequency;

CREATE OR REPLACE VIEW
Visualization2.sun_frequency AS
SELECT * FROM veloup.sun_frequency;

GRANT SELECT ON Visualization2.*
TO gui@localhost;

FLUSH PRIVILEGES;

GRANT SELECT ON db.* TO gui@localhost;
FLUSH PRIVILEGES;

#############################################################################3
DROP VIEW visualization2.wind_freqency;
#Visualization

# Query 1 - most frequent siteName
SELECT
SiteName,
TrafficTyp, 
Total,
YearValue,
MonthValue,
Weekday
FROM traffic_frequency
WHERE TrafficTyp IS NOT NULL
GROUP BY SiteName, YearValue, MonthValue, Weekday, TrafficTyp
ORDER BY Total desc;

############################################################################3

#Changing Character types - to help visualizations
ALTER TABLE TRAFFIC_TIME
DROP CONSTRAINT CK_Year;

ALTER TABLE TRAFFIC_TIME
MODIFY YearValue VARCHAR(4) NOT NULL;

ALTER TABLE TRAFFIC_TIME
DROP CONSTRAINT CK_Month;

ALTER TABLE TRAFFIC_TIME
MODIFY MonthValue VARCHAR(2) NOT NULL;

ALTER TABLE TRAFFIC_TIME
DROP CONSTRAINT CK_Weekday;

ALTER TABLE TRAFFIC_TIME
MODIFY Weekday VARCHAR(1) NOT NULL;

ALTER TABLE TRAFFIC_TIME
DROP CONSTRAINT CK_Day;

ALTER TABLE TRAFFIC_TIME
MODIFY DayValue VARCHAR(2) NOT NULL;


#########################################################################

SELECT ROUND(avg_sun) FROM rain_frequency WHERE avg_precip > 0;

