use etldb;
CREATE TABLE IF NOT EXISTS subsriber_history(
CALL_ORIGINATING_NUMBER STRING
,FIRST_CALL_START_DATE STRING
,LAST_CALL_START_DATE STRING
)
STORED AS ORC;

insert into subsriber_history
select CALL_ORIGINATING_NUMBER, min(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CALL_START_DATE, 'yyyy-MM-dd')))), max(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CALL_START_DATE, 'yyyy-MM-dd')))) from mno_cdr_data_cleansed group by CALL_ORIGINATING_NUMBER ;

 use etldb;
 CREATE TABLE IF NOT EXISTS Active_subsriber_history(
 CALL_ORIGINATING_NUMBER STRING
 ,FIRST_CALL_START_DATE STRING
 ,LAST_CALL_START_DATE STRING
 ,Active_subs STRING
 )
 STORED AS ORC;
 
 insert into Active_subsriber_history
 select CALL_ORIGINATING_NUMBER,FIRST_CALL_START_DATE,LAST_CALL_START_DATE, 
 case 
 when
 LAST_CALL_START_DATE > '2018-01-31' THEN 'Y' ELSE 'N' END as active_ind
  from  subsriber_history ;

CREATE TABLE IF NOT EXISTS active_cdr(
USAGE_TYPE_NAME STRING
,CALL_ORIGINATING_NUMBER STRING
,CALL_TERMINATING_NUMBER STRING
,CALL_START_DATE STRING
,CALL_START_TIME STRING
,Duration String
,LAC_ID STRING
,CELL_ID STRING)
COMMENT 'active cdr'      
STORED AS ORC;

 insert into active_cdr 
 select a.* from mno_cdr_data_cleansed a inner join Active_subsriber_history b
 on a.CALL_ORIGINATING_NUMBER = b.CALL_ORIGINATING_NUMBER 
 and b.Active_subs ='Y' ;