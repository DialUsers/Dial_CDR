use etldb;
CREATE TABLE etldb.mno_7am_7pm(
usage_type_name string,
call_originating_number string,
call_terminating_number string,
call_start_date date,
call_start_time timestamp,
duration string,
lac_id string,
cell_id string);

insert into etldb.mno_7am_7pm
SELECT * FROM etldb.active_cdr_cleansed where HOUR(call_start_time)>=07 OR HOUR(call_start_time)<19;
