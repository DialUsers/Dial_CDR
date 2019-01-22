use etldb;
create table etldb.active_4_hr_bulk(
call_originating_number  string,
call_start_date date,
no_of_events int
);

insert into etldb.active_4_hr_bulk 
select call_originating_number,call_start_date, count(call_originating_number) from etldb.short_term_pop_level_agg group by call_originating_number,call_start_date having count(call_originating_number) > 200



create table etldb.active_4_hr_filtered(
usage_type_name  string,  
call_originating_number  string,
call_terminating_number  string, 
call_start_date date,    
call_start_time  timestamp,       
duration       string,  
lac_id   string,  
cell_id  string
) ;
INSERT OVERWRITE TABLE etldb.active_4_hr_filtered
SELECT a.usage_type_name,a.call_originating_number,a.call_terminating_number,a.call_start_date,a.call_start_time,a.duration,a.lac_id,a.cell_id FROM etldb.short_term_pop_level_agg a
LEFT JOIN etldb.active_4_hr_bulk B
ON A.CALL_ORIGINATING_NUMBER=B.CALL_ORIGINATING_NUMBER
where B.CALL_ORIGINATING_NUMBER IS NULL;


create table etldb.active_4_hr_filtered_part(
usage_type_name  string,  
call_originating_number  string,
call_terminating_number  string,   
call_start_time  timestamp,       
duration       string,  
lac_id   string,  
cell_id  string
)
partitioned by(call_start_date date);

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

insert into etldb.active_4_hr_filtered_part  partition(call_start_date)
select usage_type_name,call_originating_number,call_terminating_number,call_start_time,duration,lac_id,cell_id,call_start_date from etldb.active_4_hr_filtered ;




create table etldb.short_term_pop_14days(
usage_type_name  string,  
call_originating_number  string,
call_terminating_number  string,   
call_start_time  timestamp,       
duration       string,  
lac_id   string,  
cell_id  string
)
partitioned by (call_start_date date) ;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

insert into etldb.short_term_pop_14days partition(call_start_date)
select usage_type_name,call_originating_number,call_terminating_number,call_start_time,duration,lac_id,cell_id,call_start_date from etldb.active_4_hr_filtered_part where call_start_date >= "2018-05-06" and call_start_date <= "2018-05-12" and HOUR(call_start_time)>= 07 AND HOUR(call_start_time)< 21  ; 

insert into etldb.short_term_pop_14days partition(call_start_date)
select usage_type_name,call_originating_number,call_terminating_number,call_start_time,duration,lac_id,cell_id,call_start_date from etldb.active_4_hr_filtered_part where call_start_date >= "2018-04-22" and call_start_date <= "2018-04-28" and HOUR(call_start_time)>= 07 AND HOUR(call_start_time)< 21  ; 


******************************************************************************************************************************************************************************
use etldb;
CREATE TABLE short_term_pop_14days_Timeslot(
  usage_type_name string, 
  call_originating_number string, 
  call_terminating_number string, 
  call_start_time timestamp, 
  duration string, 
  lac_id string, 
  cell_id string,
  call_start_date date,
  Timeslot string
  );
PARTITIONED BY (Timeslot string,call_start_date date)
  


INSERT into table etldb.short_term_pop_14days_Timeslot
select usage_type_name,call_originating_number,call_terminating_number,call_start_time,duration,lac_id,cell_id,call_start_date,
case
WHEN hour(call_start_time) >= 7 and hour(call_start_time) <9
THEN '7AM-9AM'
WHEN hour(call_start_time) >= 9 and hour(call_start_time) < 11
THEN '9AM-11AM'
WHEN hour(call_start_time) >= 11 and hour(call_start_time) < 13
THEN '11AM-13PM'
WHEN hour(call_start_time) >= 13 and hour(call_start_time) < 15
THEN '13PM-15PM'
WHEN hour(call_start_time) >= 15 and hour(call_start_time) < 17
THEN '15PM-17PM'
WHEN hour(call_start_time) >= 17 and hour(call_start_time) < 19
THEN '17PM-19PM'
WHEN hour(call_start_time) >= 19 and hour(call_start_time) < 21
THEN '19PM-21PM'
ELSE 'NA'
END
from short_term_pop_14days;

*******************************************************************************************************************************************************************************


CREATE TABLE IF NOT EXISTS short_term_lvl1(
CALL_ORIGINATING_NUMBER STRING
,CALL_START_DATE STRING
,timeslot STRING
,CELL_ID STRING
,NO_OF_EVENTS string

);

INSERT INTO short_term_lvl1 
SELECT CALL_ORIGINATING_NUMBER,CALL_START_DATE,timeslot,CELL_ID,count(CALL_ORIGINATING_NUMBER)as NO_OF_EVENTS from short_term_pop_14days_Timeslot group by CALL_ORIGINATING_NUMBER,CALL_START_DATE,timeslot,CELL_ID;

Create table --

CREATE TABLE IF NOT EXISTS short_term_Master(
CALL_ORIGINATING_NUMBER STRING
,CALL_START_DATE STRING
,CELL_ID STRING
,timeslot STRING
,RNK INT

);

insert into short_term_Master 
SELECT RNK.CALL_ORIGINATING_NUMBER,RNK.CALL_START_DATE,RNK.CELL_ID,timeslot,RNK  FROM
(
SELECT CALL_ORIGINATING_NUMBER,CALL_START_DATE,CELL_ID,timeslot, NO_OF_EVENTS,RANK() OVER (PARTITION  BY CALL_ORIGINATING_NUMBER,CALL_START_DATE,timeslot ORDER BY NO_OF_EVENTS DESC ) AS RNK
FROM short_term_lvl1  GROUP BY CALL_ORIGINATING_NUMBER,CALL_START_DATE,timeslot,CELL_ID,NO_OF_EVENTS ORDER BY CALL_START_DATE )RNK
WHERE RNK.RNK=1 ;