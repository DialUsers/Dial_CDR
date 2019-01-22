use etldb;
CREATE EXTERNAL TABLE IF NOT EXISTS mno_data(
            USAGE_TYPE_NAME STRING
    ,CALL_ORIGINATING_NUMBER STRING
    ,CALL_TERMINATING_NUMBER STRING
    ,CALL_START_DATE STRING
    ,CALL_START_TIME STRING
    ,Duration String
    ,LAC_ID STRING
    ,CELL_ID STRING)
    COMMENT 'Test data'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    location 'adl://dialmalawi.azuredatalakestore.net/clusters/DIALMalawi/mno_raw_data_anon/';
