use etldb;
CREATE TABLE mno_cdr_data_cleansed(
usage_type_name string,
call_originating_number string,
call_terminating_number string,
call_start_date date,
call_start_time timestamp,
duration string,
lac_id string,
cell_id string);

insert into  mno_cdr_data_cleansed
select usage_type_name, call_originating_number, call_terminating_number, TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CALL_START_DATE, 'yyyy-MM-dd'))), concat(call_start_date,' ', concat(concat(substr(call_start_time,0,2),':',substr(call_start_time,4,2)),':',substr(call_start_time,7,2))) ,NULL,lac_id,cell_id from mno_data_comp ;
