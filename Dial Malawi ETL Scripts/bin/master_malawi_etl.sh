####################################################################
#    Script Name: master_malawi_etl.sh 							               #	
#	 Description: Master script  which calls individual SQL for      #
#				  aggregation and data filteration                         #
#            										                                   #
####################################################################
"""
Created on Mon Jan 7 02:55:38 2019

@author: Infosys
"""


SCHEMA="etldb"
TODAY =$(date "+%Y%m%d")
#target table names for loading purpose
TARGETTABLE_test=test
TARGETTABLE_1=mno_data
TARGETTABLE_2=mno_data_comp
TARGETTABLE_3=mno_cdr_data_cleansed
TARGETTABLE_4=active_cdr_cleansed
TARGETTABLE_5=mno_7am_7pm
TARGETTABLE_6=AGG_DAY_WISE_7am_7pm
TARGETTABLE_7=AGG_DAY_WISE_TIME_7am_7pm
TARGETTABLE_8=AGG_MONTH_WISE_7am_7pm
TARGETTABLE_9=HOMELOCATION_DAY_STEP1_7am_7pm
TARGETTABLE_10=HOMELOCATION_DAY_STEP2_7am_7pm
TARGETTABLE_11=HOMELOCATION_DAY_7am_7pm
TARGETTABLE_12=MNO_DAILY_AGG_cdr_7am_7pm
TARGETTABLE_13=MNO_DAILY_AGG_home_7am_7pm
TARGETTABLE_14=MNO_DAILY_AGG_7am_7pm
TARGETTABLE_15=subs_record_four_hr_active
TARGETTABLE_16=subs_4_hr_lat_long
TARGETTABLE_17=subs_4_hr_lat_long_3months
TARGETTABLE_18=subs_4_hr_lat_long_3months_null
TARGETTABLE_19=short_term_pop_level_agg
TARGETTABLE_20=active_4_hr_bulk
TARGETTABLE_21=active_4_hr_filtered
TARGETTABLE_22=active_4_hr_filtered_part
TARGETTABLE_23=short_term_pop_14days
TARGETTABLE_24=short_term_pop_14days_Timeslot
TARGETTABLE_25=short_term_lvl1
TARGETTABLE_26=short_term_Master
email_id= /home/dialssh/dev/mail_info/email.txt

#LOG FUNCTION : 
log() {
        echo "[`date '+%m%d%Y:%H:%M:%S'`]:$1"
}


# Create External Hive table on ADL path 'adl://dialmalawi.azuredatalakestore.net/clusters/DIALMalawi/mno_raw_data_anon/'
nohup spark-sql -f /home/dialssh/dev/agg_on_MNO_FULL_DATA/1_ext_table_mno_data_ddl.sql 2> /home/dialssh/log/1_ext_table_mno_data_ddl.log 

#status check if spark command executed successfully or failed
if [ `echo $?` -ne 0 ];then
        log "Load process for ${SCHEMA}.${TARGETTABLE_1} Failed..."
		#Mail notification for process failure and exit the script
		echo "Load process for ${SCHEMA}.${TARGETTABLE_1} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
#Load process succeeded
log "Load process for ${SCHEMA}.${TARGETTABLE_1} Done: Process succeeded... " 

#create ORC table on top of the above created external hive table
nohup spark-sql -f /home/dialssh/dev/agg_on_MNO_FULL_DATA/2_mngd_table_mno_data_ddl.sql 2> /home/dialssh/log/2_mngd_table_mno_data_ddl.log 
#status check if spark command executed successfully or failed
if [ `echo $?` -ne 0 ];then
        log "Load process for ${SCHEMA}.${TARGETTABLE_2} Failed..."
		#Mail notification for process failure and exit the script
		echo "Load process for ${SCHEMA}.${TARGETTABLE_2} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
#Load process succeeded
log "Load process for ${SCHEMA}.${TARGETTABLE_2} Done: Process succeeded... " 

# Fix timestamp format and cleanse data to have cell_id not null 
nohup spark-sql -f /home/dialssh/dev/agg_on_MNO_FULL_DATA/mno_cdr_data_cleansed.sql 2> /home/dialssh/log/mno_cdr_data_cleansed.log 
if [ `echo $?` -ne 0 ];then
        log "Load process for ${SCHEMA}.${TARGETTABLE_3} Failed..."
		echo "Load process for ${SCHEMA}.${TARGETTABLE_3} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
log "Load process for ${SCHEMA}.${TARGETTABLE_3} Done: Process succeeded... " 

#filter active records: Records which have been used consecutively for past three months
nohup spark-sql -f /home/dialssh/dev/active_inactive_dev/active_users.sql 2> /home/dialssh/log/active_users.log 

if [ `echo $?` -ne 0 ];then
        log "Load process for ${SCHEMA}.${TARGETTABLE_4} Failed..."
		echo "Load process for ${SCHEMA}.${TARGETTABLE_4} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
log "Load process for ${SCHEMA}.${TARGETTABLE_4} Done: Process succeeded... " 

#Filter records to contain data between 7am and 7pm 
nohup spark-sql -f /home/dialssh/dev/mno_MORNING/mno_7am_7pm.sql 2> /home/dialssh/log/mno_7am_7pm.sql.log
if [ `echo $?` -ne 0 ];then
        log "Load process for ${SCHEMA}.${TARGETTABLE_5} Failed..."
		echo "Load process for ${SCHEMA}.${TARGETTABLE_5} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
log "Load process for ${SCHEMA}.${TARGETTABLE_5} Done: Process succeeded... " 

#aggregation_rule: Day wise aggregation on each subscriber at cell_id level
nohup spark-sql -f /home/dialssh/dev/agg_on_7am_7pm/AGG_DAY_WISE_7am_7pm.sql 2> /home/dialssh/log/AGG_DAY_WISE_7am_7pm.log
if [ `echo $?` -ne 0 ];then
        log "Load process for ${SCHEMA}.${TARGETTABLE_6} Failed..."
		echo "Load process for ${SCHEMA}.${TARGETTABLE_6} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
log "Load process for ${SCHEMA}.${TARGETTABLE_6} Done: Process succeeded... " 

#aggregation rule: Day wise aggregation on each subscriber at cell_id level with the latestcall 
nohup spark-sql -f /home/dialssh/dev/agg_on_7am_7pm/AGG_DAY_WISE_TIME_7am_7pm.sql 2> /home/dialssh/log/AGG_DAY_WISE_TIME_7am_7pm.log
if [ `echo $?` -ne 0 ];then
        log "Load process for ${SCHEMA}.${TARGETTABLE_7} Failed..."
		echo "Load process for ${SCHEMA}.${TARGETTABLE_7} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
log "Load process for ${SCHEMA}.${TARGETTABLE_7} Done: Process succeeded... " 

#aggregation rule: Month wise aggregation on each subscriber at cell_id level
nohup spark-sql -f /home/dialssh/dev/agg_on_7am_7pm/AGG_MONTH_WISE_7am_7pm.sql 2> /home/dialssh/log/AGG_MONTH_WISE_7am_7pm.log
if [ `echo $?` -ne 0 ];then
        log "Load process for ${SCHEMA}.${TARGETTABLE_8} Failed..."
		echo "Load process for ${SCHEMA}.${TARGETTABLE_8} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
log "Load process for ${SCHEMA}.${TARGETTABLE_8} Done: Process succeeded... " 

#aggregation rule: Homelocation allocated to each active subscriber 
nohup spark-sql -f /home/dialssh/dev/agg_on_7am_7pm/HOMELOCATION_DAY_STEP1_7am_7pm.sql 2> /home/dialssh/log/HOMELOCATION_DAY_STEP1_7am_7pm.log
if [ `echo $?` -ne 0 ];then
        log "Load process for ${SCHEMA}.${TARGETTABLE_9} Failed..."
		echo "Load process for ${SCHEMA}.${TARGETTABLE_9} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
log "Load process for ${SCHEMA}.${TARGETTABLE_9} Done: Process succeeded... " 

#aggregation rule: Contains homelocation for each subscriber mapped with last call made from a particular tower
nohup spark-sql -f /home/dialssh/dev/agg_on_7am_7pm/HOMELOCATION_DAY_STEP2_7am_7pm.sql 2> /home/dialssh/log/HOMELOCATION_DAY_STEP2_7am_7pm.log
if [ `echo $?` -ne 0 ];then
        log "Load process for ${SCHEMA}.${TARGETTABLE_10} Failed..."
		echo "Load process for ${SCHEMA}.${TARGETTABLE_10} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
log "Load process for ${SCHEMA}.${TARGETTABLE_10} Done: Process succeeded... " 

#aggregation rule: Homelocation allocated to each active subscriber based on last call location
nohup spark-sql -f /home/dialssh/dev/agg_on_7am_7pm/HOMELOCATION_DAY_7am_7pm.sql 2> /home/dialssh/log/HOMELOCATION_DAY_7am_7pm.log
if [ `echo $?` -ne 0 ];then
        log "Load process for ${SCHEMA}.${TARGETTABLE_11} Failed..."
		echo "Load process for ${SCHEMA}.${TARGETTABLE_11} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
log "Load process for ${SCHEMA}.${TARGETTABLE_11} Done: Process succeeded... " 

#aggregation rule: Day wise count of events at each tower
nohup spark-sql -f /home/dialssh/dev/agg_on_7am_7pm/MNO_DAILY_AGG_cdr_7am_7pm.sql 2> /home/dialssh/log/MNO_DAILY_AGG_cdr_7am_7pm.log
if [ `echo $?` -ne 0 ];then
        log "Load process for ${SCHEMA}.${TARGETTABLE_12} Failed..."
		echo "Load process for ${SCHEMA}.${TARGETTABLE_12} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
log "Load process for ${SCHEMA}.${TARGETTABLE_12} Done: Process succeeded... " 

#aggregation rule: Distinct day wise records for each cell_id
nohup spark-sql -f /home/dialssh/dev/agg_on_7am_7pm/MNO_DAILY_AGG_home_7am_7pm.sql 2> /home/dialssh/log/MNO_DAILY_AGG_home_7am_7pm.log
if [ `echo $?` -ne 0 ];then
        log "Load process for ${SCHEMA}.${TARGETTABLE_13} Failed..."
		echo "Load process for ${SCHEMA}.${TARGETTABLE_13} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
log "Load process for ${SCHEMA}.${TARGETTABLE_13} Done: Process succeeded... " 

#aggregation rule: Day wise aggregstion on cell_id level 
nohup spark-sql -f /home/dialssh/dev/agg_on_7am_7pm/MNO_DAILY_AGG_7am_7pm.sql 2> /home/dialssh/log/MNO_DAILY_AGG_7am_7pm.log
if [ `echo $?` -ne 0 ];then
        log "Load process for ${SCHEMA}.${TARGETTABLE_14} Failed..."
		echo "Load process for ${SCHEMA}.${TARGETTABLE_14} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
log "Load process for ${SCHEMA}.${TARGETTABLE_14} Done: Process succeeded... " 

#Short term population movement scripts
nohup spark-sql -f  /home/dialssh/dev/short_term_pop_scripts/subs_record_four_hr_active.sql 2> /home/dialssh/log/subs_record_four_hr_active.log
        log "Load process for ${SCHEMA}.${TARGETTABLE_15} Failed..."
		echo "Load process for ${SCHEMA}.${TARGETTABLE_15} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
log "Load process for ${SCHEMA}.${TARGETTABLE_15} Done: Process succeeded... " 

nohup spark-sql -f  /home/dialssh/dev/short_term_pop_scripts/subs_4_hr_lat_long.sql 2> /home/dialssh/log/subs_4_hr_lat_long.log
        log "Load process for ${SCHEMA}.${TARGETTABLE_16} Failed..."
		echo "Load process for ${SCHEMA}.${TARGETTABLE_16} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
log "Load process for ${SCHEMA}.${TARGETTABLE_16} Done: Process succeeded... " 

nohup spark-sql -f  /home/dialssh/dev/short_term_pop_scripts/subs_4_hr_lat_long_3months.sql 2> /home/dialssh/log/subs_4_hr_lat_long_3months.log
        log "Load process for ${SCHEMA}.${TARGETTABLE_17} Failed..."
		echo "Load process for ${SCHEMA}.${TARGETTABLE_17} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
log "Load process for ${SCHEMA}.${TARGETTABLE_17} Done: Process succeeded... " 

nohup spark-sql -f  /home/dialssh/dev/short_term_pop_scripts/subs_4_hr_lat_long_3months_null.sql 2> /home/dialssh/log/subs_4_hr_lat_long_3months_null.log
        log "Load process for ${SCHEMA}.${TARGETTABLE_18} Failed..."
		echo "Load process for ${SCHEMA}.${TARGETTABLE_18} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
log "Load process for ${SCHEMA}.${TARGETTABLE_18} Done: Process succeeded... " 

nohup spark-sql -f  /home/dialssh/dev/short_term_pop_scripts/short_term_pop_level_agg.sql 2> /home/dialssh/log/short_term_pop_level_agg.log
if [ `echo $?` -ne 0 ];then
        log "Load process for ${SCHEMA}.${TARGETTABLE_19} Failed..."
		echo "Load process for ${SCHEMA}.${TARGETTABLE_19} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
log "Load process for ${SCHEMA}.${TARGETTABLE_19} Done: Process succeeded... " 

#Filter records with bulk sms , more than 200 calls per day
nohup spark-sql -f  /home/dialssh/dev/short_term_pop_scripts/active_4_hr_bulk.sql 2> /home/dialssh/log/active_4_hr_bulk.log
if [ `echo $?` -ne 0 ];then
        log "Load process for ${SCHEMA}.${TARGETTABLE_20} Failed..."
		echo "Load process for ${SCHEMA}.${TARGETTABLE_20} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
log "Load process for ${SCHEMA}.${TARGETTABLE_20} Done: Process succeeded... " 

#Filtering table to contain non bulk records alone 
nohup spark-sql -f  /home/dialssh/dev/short_term_pop_scripts/active_4_hr_filtered.sql 2> /home/dialssh/log/active_4_hr_filtered.log
if [ `echo $?` -ne 0 ];then
        log "Load process for ${SCHEMA}.${TARGETTABLE_21} Failed..."
		echo "Load process for ${SCHEMA}.${TARGETTABLE_21} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
log "Load process for ${SCHEMA}.${TARGETTABLE_21} Done: Process succeeded... " 

#Loading filtered data into partitioned table
nohup spark-sql -f  /home/dialssh/dev/short_term_pop_scripts/active_4_hr_filtered_part.sql 2> /home/dialssh/log/active_4_hr_filtered_part.log
if [ `echo $?` -ne 0 ];then
        log "Load process for ${SCHEMA}.${TARGETTABLE_22} Failed..."
		echo "Load process for ${SCHEMA}.${TARGETTABLE_22} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
log "Load process for ${SCHEMA}.${TARGETTABLE_22} Done: Process succeeded... " 

#Loading one weeks data of rainy and non-rainy season
nohup spark-sql -f  /home/dialssh/dev/short_term_pop_scripts/short_term_pop_14days.sql 2> /home/dialssh/log/short_term_pop_14days.log
if [ `echo $?` -ne 0 ];then
        log "Load process for ${SCHEMA}.${TARGETTABLE_23} Failed..."
		echo "Load process for ${SCHEMA}.${TARGETTABLE_23} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
log "Load process for ${SCHEMA}.${TARGETTABLE_23} Done: Process succeeded... " 

#Partitionong the above table with date and pivot using timeslot (consecutive 2 hour records) 
nohup spark-sql -f  /home/dialssh/dev/short_term_pop_scripts/short_term_pop_14days_Timeslot.sql 2> /home/dialssh/log/short_term_pop_14days_Timeslot.log
if [ `echo $?` -ne 0 ];then
        log "Load process for ${SCHEMA}.${TARGETTABLE_24} Failed..."
		echo "Load process for ${SCHEMA}.${TARGETTABLE_24} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
log "Load process for ${SCHEMA}.${TARGETTABLE_24} Done: Process succeeded... " 

#Short_term_lvl1 aggregation 
nohup spark-sql -f  /home/dialssh/dev/short_term_pop_scripts/short_term_lvl1.sql 2> /home/dialssh/log/short_term_lvl1.log
if [ `echo $?` -ne 0 ];then
        log "Load process for ${SCHEMA}.${TARGETTABLE_25} Failed..."
		echo "Load process for ${SCHEMA}.${TARGETTABLE_25} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
log "Load process for ${SCHEMA}.${TARGETTABLE_25} Done: Process succeeded... " 

#Short term master table: Final table 
nohup spark-sql -f  /home/dialssh/dev/short_term_pop_scripts/short_term_Master.sql 2> /home/dialssh/log/short_term_Master.log
if [ `echo $?` -ne 0 ];then
        log "Load process for ${SCHEMA}.${TARGETTABLE_26} Failed..."
		echo "Load process for ${SCHEMA}.${TARGETTABLE_26} Failed..." | mail -s "Failure" joanna.varghese@infosys.com praveenkumar.sekar@infosys.com
        exit 1
fi
log "Load process for ${SCHEMA}.${TARGETTABLE_26} Done: Process succeeded... " 
