use etldb;
create table if not exists short_term_pop_level1_agg (
usage_type_name string  , 
call_originating_number string  , 
call_terminating_number string ,  
call_start_date date    , 
call_start_time timestamp,        
duration        string ,  
lac_id  string   ,
cell_id string  , 
LAT     string ,  
LONG    string   ) ;

insert into etldb.short_term_pop_level1_agg
select a.* from advanceanalyticsdb.subs_4_hr_lat_long_3months a left join advanceanalyticsdb.subs_4_hr_lat_long_3months_null b ON a.call_originating_number=b.call_originating_number and a.call_start_date=b.call_start_date where
b.call_originating_number is null or b.call_start_date is null;

