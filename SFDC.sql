--INSERT INTO SANDBOX_MOP_CUST_ANALYTICS.CUST_ANALYTICS.MP_PIPELINE_HEALTH
(
  
SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'SFDC' "SCHEMA"
,'T_S_SFDC_OPPORTUNITY_LINE_ITEM' "Table"
,aircont.TASK_ID
,aircont.DAG_ID
,aircont.START_DATE
,aircont.END_DATE
,aircont.DURATION
,aircont.state
,cont.Last_modified
,cont.Row_size
,col.cou Column_Size
,mbs.mbs

FROM (SELECT TASK_ID
        ,DAG_ID
        ,START_DATE
        ,END_DATE
        ,DURATION
        ,state
      FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
      WHERE DAG_ID ='sfdc-pipeline'
      AND TASK_ID = 'load_t_s_sfdc_opportunity_line_item'
      AND START_date > '2022-04-21 00:00:00') AirCont

JOIN (SELECT max(UPDATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_content) Cont
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_ODS.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'SFDC'
		AND TABLE_NAME = 'T_S_SFDC_OPPORTUNITY_LINE_ITEM') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_ODS.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'SFDC'
		AND TABLE_NAME = 'T_S_SFDC_OPPORTUNITY_LINE_ITEM') mbs
		
  union	

  SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'SFDC' "SCHEMA"
,'T_S_SFDC_PRODUCT2' "Table"
,aircont.TASK_ID
,aircont.DAG_ID
,aircont.START_DATE
,aircont.END_DATE
,aircont.DURATION
,aircont.state
,cont.Last_modified
,cont.Row_size
,col.cou Column_Size
,mbs.mbs

FROM (SELECT TASK_ID
        ,DAG_ID
        ,START_DATE
        ,END_DATE
        ,DURATION
        ,state
      FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
      WHERE DAG_ID ='sfdc-pipeline'
      AND TASK_ID = 'load_t_s_sfdc_product2'
      AND START_date > '2022-04-21 00:00:00') AirCont

JOIN (SELECT max(UPDATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_content) Cont
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_ODS.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'SFDC'
		AND TABLE_NAME = 'T_S_SFDC_PRODUCT2') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_ODS.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'SFDC'
		AND TABLE_NAME = 'T_S_SFDC_PRODUCT2') mbs
  
  union
  
    SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'SFDC' "SCHEMA"
,'T_S_SFDC_OPPORTUNITY' "Table"
,aircont.TASK_ID
,aircont.DAG_ID
,aircont.START_DATE
,aircont.END_DATE
,aircont.DURATION
,aircont.state
,cont.Last_modified
,cont.Row_size
,col.cou Column_Size
,mbs.mbs

FROM (SELECT TASK_ID
        ,DAG_ID
        ,START_DATE
        ,END_DATE
        ,DURATION
        ,state
      FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
      WHERE DAG_ID ='sfdc-pipeline'
      AND TASK_ID = 'load_t_s_sfdc_opportunity'
      AND START_date > '2022-04-21 00:00:00') AirCont

JOIN (SELECT max(UPDATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_content) Cont
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_ODS.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'SFDC'
		AND TABLE_NAME = 'T_S_SFDC_OPPORTUNITY') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_ODS.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'SFDC'
		AND TABLE_NAME = 'T_S_SFDC_OPPORTUNITY') mbs
  
  union
  
   
    SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'SFDC' "SCHEMA"
,'T_S_SFDC_CONTACT' "Table"
,aircont.TASK_ID
,aircont.DAG_ID
,aircont.START_DATE
,aircont.END_DATE
,aircont.DURATION
,aircont.state
,cont.Last_modified
,cont.Row_size
,col.cou Column_Size
,mbs.mbs

FROM (SELECT TASK_ID
        ,DAG_ID
        ,START_DATE
        ,END_DATE
        ,DURATION
        ,state
      FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
      WHERE DAG_ID ='sfdc-pipeline'
      AND TASK_ID = 'load_t_s_sfdc_contact'
      AND START_date > '2022-04-21 00:00:00') AirCont

JOIN (SELECT max(UPDATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_content) Cont
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_ODS.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'SFDC'
		AND TABLE_NAME = 'T_S_SFDC_CONTACT') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_ODS.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'SFDC'
		AND TABLE_NAME = 'T_S_SFDC_CONTACT') mbs
  
 union
  
   SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'SFDC' "SCHEMA"
,'T_S_SFDC_TASK' "Table"
,aircont.TASK_ID
,aircont.DAG_ID
,aircont.START_DATE
,aircont.END_DATE
,aircont.DURATION
,aircont.state
,cont.Last_modified
,cont.Row_size
,col.cou Column_Size
,mbs.mbs

FROM (SELECT TASK_ID
        ,DAG_ID
        ,START_DATE
        ,END_DATE
        ,DURATION
        ,state
      FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
      WHERE DAG_ID ='sfdc-pipeline'
      AND TASK_ID = 'load_t_s_sfdc_task'
      AND START_date > '2022-04-21 00:00:00') AirCont

JOIN (SELECT max(UPDATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_content) Cont
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_ODS.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'SFDC'
		AND TABLE_NAME = 'T_S_SFDC_TASK') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_ODS.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'SFDC'
		AND TABLE_NAME = 'T_S_SFDC_TASK') mbs
  
  union
  
   
   SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'SFDC' "SCHEMA"
,'T_S_SFDC_CAMPAIGN_INFLUENCE' "Table"
,aircont.TASK_ID
,aircont.DAG_ID
,aircont.START_DATE
,aircont.END_DATE
,aircont.DURATION
,aircont.state
,cont.Last_modified
,cont.Row_size
,col.cou Column_Size
,mbs.mbs

FROM (SELECT TASK_ID
        ,DAG_ID
        ,START_DATE
        ,END_DATE
        ,DURATION
        ,state
      FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
      WHERE DAG_ID ='sfdc-pipeline'
      AND TASK_ID = 'load_t_s_sfdc_campaign_influence'
      AND START_date > '2022-04-21 00:00:00') AirCont

JOIN (SELECT max(UPDATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_content) Cont
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_ODS.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'SFDC'
		AND TABLE_NAME = 'T_S_SFDC_CAMPAIGN_INFLUENCE') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_ODS.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'SFDC'
		AND TABLE_NAME = 'T_S_SFDC_CAMPAIGN_INFLUENCE') mbs
  
union
  
   
   
   SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'SFDC' "SCHEMA"
,'T_S_SFDC_CAMPAIGN_MEMBER' "Table"
,aircont.TASK_ID
,aircont.DAG_ID
,aircont.START_DATE
,aircont.END_DATE
,aircont.DURATION
,aircont.state
,cont.Last_modified
,cont.Row_size
,col.cou Column_Size
,mbs.mbs

FROM (SELECT TASK_ID
        ,DAG_ID
        ,START_DATE
        ,END_DATE
        ,DURATION
        ,state
      FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
      WHERE DAG_ID ='sfdc-pipeline'
      AND TASK_ID = 'load_t_s_sfdc_campaign_member'
      AND START_date > '2022-04-21 00:00:00') AirCont

JOIN (SELECT max(UPDATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_content) Cont
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_ODS.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'SFDC'
		AND TABLE_NAME = 'T_S_SFDC_CAMPAIGN_MEMBER') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_ODS.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'SFDC'
		AND TABLE_NAME = 'T_S_SFDC_CAMPAIGN_MEMBER') mbs
  
  union
  
   SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'SFDC' "SCHEMA"
,'T_S_SFDC_CAMPAIGN' "Table"
,aircont.TASK_ID
,aircont.DAG_ID
,aircont.START_DATE
,aircont.END_DATE
,aircont.DURATION
,aircont.state
,cont.Last_modified
,cont.Row_size
,col.cou Column_Size
,mbs.mbs

FROM (SELECT TASK_ID
        ,DAG_ID
        ,START_DATE
        ,END_DATE
        ,DURATION
        ,state
      FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
      WHERE DAG_ID ='sfdc-pipeline'
      AND TASK_ID = 'load_t_s_sfdc_campaign'
      AND START_date > '2022-04-21 00:00:00') AirCont

JOIN (SELECT max(UPDATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_content) Cont
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_ODS.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'SFDC'
		AND TABLE_NAME = 'T_S_SFDC_CAMPAIGN') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_ODS.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'SFDC'
		AND TABLE_NAME = 'T_S_SFDC_CAMPAIGN') mbs
  
  union
  
  ------- below tables no relationship each other in diagramma
  
    SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'SFDC' "SCHEMA"
,'T_S_SFDC_REC_TYPE' "Table"
,aircont.TASK_ID
,aircont.DAG_ID
,aircont.START_DATE
,aircont.END_DATE
,aircont.DURATION
,aircont.state
,cont.Last_modified
,cont.Row_size
,col.cou Column_Size
,mbs.mbs

FROM (SELECT TASK_ID
        ,DAG_ID
        ,START_DATE
        ,END_DATE
        ,DURATION
        ,state
      FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
      WHERE DAG_ID ='sfdc-pipeline'
      AND TASK_ID = 'load_t_s_sfdc_rec_type'
      AND START_date > '2022-04-21 00:00:00') AirCont

JOIN (SELECT max(UPDATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_content) Cont
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_ODS.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'SFDC'
		AND TABLE_NAME = 'T_S_SFDC_REC_TYPE') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_ODS.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'SFDC'
		AND TABLE_NAME = 'T_S_SFDC_REC_TYPE') mbs
  
  union
 
      SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'SFDC' "SCHEMA"
,'T_S_SFDC_USER' "Table"
,aircont.TASK_ID
,aircont.DAG_ID
,aircont.START_DATE
,aircont.END_DATE
,aircont.DURATION
,aircont.state
,cont.Last_modified
,cont.Row_size
,col.cou Column_Size
,mbs.mbs

FROM (SELECT TASK_ID
        ,DAG_ID
        ,START_DATE
        ,END_DATE
        ,DURATION
        ,state
      FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
      WHERE DAG_ID ='sfdc-pipeline'
      AND TASK_ID = 'load_t_s_sfdc_user'
      AND START_date > '2022-04-21 00:00:00') AirCont

JOIN (SELECT max(UPDATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_content) Cont
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_ODS.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'SFDC'
		AND TABLE_NAME = 'T_S_SFDC_USER') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_ODS.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'SFDC'
		AND TABLE_NAME = 'T_S_SFDC_USER') mbs
 
  
)
;
select * from (
select distinct task_id FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
where  task_id LIKE '%user%'
  )a
  where task_id LIKE '%load%'
  ;      