--INSERT INTO SANDBOX_MOP_CUST_ANALYTICS.CUST_ANALYTICS.MP_PIPELINE_HEALTH

(


SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'ELOQUA' "SCHEMA"
,'ACCOUNT' "Table"
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

FROM (SELECT 
		TASK_ID
		,DAG_ID
		,START_DATE
		,END_DATE
		,DURATION
		,state
		
		FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
		
		WHERE DAG_ID IN ('eloqua_api_pipeline')
		AND task_id = 'load_t_s_account'
		AND start_date LIKE '%2022-04-05%') AirCont

JOIN (SELECT max(UPDATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_content) Cont
		
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_EDW.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'ACCOUNT') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_EDW.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'ACCOUNT') mbs
		
union	
		
	SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'ELOQUA' "SCHEMA"
,'BOUNCEBACK' "Table"
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

FROM (SELECT 
		TASK_ID
		,DAG_ID
		,START_DATE
		,END_DATE
		,DURATION
		,state
		
		FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
		
		WHERE DAG_ID IN ('eloqua_api_pipeline')
		AND task_id = 'load_t_s_bounceback'
		AND start_date LIKE '%2022-04-05%') AirCont

JOIN (SELECT max(UPDATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_custom_fields) Cont

JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_EDW.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'BOUNCEBACK') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_EDW.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'BOUNCEBACK') mbs
		
UNION

SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'ELOQUA' "SCHEMA"
,'CAMPAIGN' "Table"
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

FROM (SELECT 
		TASK_ID
		,DAG_ID
		,START_DATE
		,END_DATE
		,DURATION
		,state
		
		FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
		
		WHERE DAG_ID IN ('eloqua_api_pipeline')
		AND task_id = 'load_t_s_campaign'
		AND start_date LIKE '%2022-04-05%') AirCont

JOIN (SELECT max(INITIATIVE_CREATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_initiatives) Cont
		
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_EDW.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'CAMPAIGN') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_EDW.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'CAMPAIGN') mbs
  
  UNION

SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'ELOQUA' "SCHEMA"
,'CONTACT' "Table"
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

FROM (SELECT 
		TASK_ID
		,DAG_ID
		,START_DATE
		,END_DATE
		,DURATION
		,state
		
		FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
		
		WHERE DAG_ID IN ('eloqua_api_pipeline')
		AND task_id = 'load_contact'
		AND start_date LIKE '%2022-04-05%') AirCont

JOIN (SELECT max(INITIATIVE_CREATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_initiatives) Cont
		
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_EDW.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'CONTACT') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_EDW.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'CONTACT') mbs
  
  UNION

SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'ELOQUA' "SCHEMA"
,'EMAILCLICKTHROUGH' "Table"
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

FROM (SELECT 
		TASK_ID
		,DAG_ID
		,START_DATE
		,END_DATE
		,DURATION
		,state
		
		FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
		
		WHERE DAG_ID IN ('eloqua_api_pipeline')
		AND task_id = 'load_t_s_emailclickthrough'
		AND start_date LIKE '%2022-04-05%') AirCont

JOIN (SELECT max(INITIATIVE_CREATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_initiatives) Cont
		
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_EDW.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'EMAILCLICKTHROUGH') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_EDW.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'EMAILCLICKTHROUGH') mbs
  
  UNION

SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'ELOQUA' "SCHEMA"
,'EMAILOPEN' "Table"
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

FROM (SELECT 
		TASK_ID
		,DAG_ID
		,START_DATE
		,END_DATE
		,DURATION
		,state
		
		FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
		
		WHERE DAG_ID IN ('eloqua_api_pipeline')
		AND task_id = 'load_t_s_emailopen'
		AND start_date LIKE '%2022-04-05%') AirCont

JOIN (SELECT max(INITIATIVE_CREATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_initiatives) Cont
		
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_EDW.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'EMAILOPEN') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_EDW.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'EMAILOPEN') mbs
  
  UNION

SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'ELOQUA' "SCHEMA"
,'EMAILSEND' "Table"
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

FROM (SELECT 
		TASK_ID
		,DAG_ID
		,START_DATE
		,END_DATE
		,DURATION
		,state
		
		FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
		
		WHERE DAG_ID IN ('eloqua_api_pipeline')
		AND task_id = 'load_t_s_emailsend'
		AND start_date LIKE '%2022-04-05%') AirCont

JOIN (SELECT max(INITIATIVE_CREATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_initiatives) Cont
		
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_EDW.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'EMAILSEND') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_EDW.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'EMAILSEND') mbs
  
  UNION

SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'ELOQUA' "SCHEMA"
,'FORMSUBMIT' "Table"
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

FROM (SELECT 
		TASK_ID
		,DAG_ID
		,START_DATE
		,END_DATE
		,DURATION
		,state
		
		FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
		
		WHERE DAG_ID IN ('eloqua_api_pipeline')
		AND task_id = 'load_t_s_formsubmit'
		AND start_date LIKE '%2022-04-05%') AirCont

JOIN (SELECT max(INITIATIVE_CREATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_initiatives) Cont
		
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_EDW.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'FORMSUBMIT') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_EDW.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'FORMSUBMIT') mbs
  
  UNION

/*SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'ELOQUA' "SCHEMA"
,'FORMSUBMIT_EXTN' "Table"
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

FROM (SELECT 
		TASK_ID
		,DAG_ID
		,START_DATE
		,END_DATE
		,DURATION
		,state
		
		FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
		
		WHERE DAG_ID IN ('eloqua_api_pipeline')
		AND task_id = 'load_t_s_sfdc_opportunity'
		AND start_date LIKE '%2022-04-05%') AirCont

JOIN (SELECT max(INITIATIVE_CREATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_initiatives) Cont
		
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_EDW.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'FORMSUBMIT_EXTN') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_EDW.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'FORMSUBMIT_EXTN') mbs
  
  UNION   */

SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'ELOQUA' "SCHEMA"
,'GLOBAL_AUTHORS_EMAIL' "Table"
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

FROM (SELECT 
		TASK_ID
		,DAG_ID
		,START_DATE
		,END_DATE
		,DURATION
		,state
		
		FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
		
		WHERE DAG_ID IN ('eloqua_api_pipeline')
		AND task_id = 'load_t_s_global_authors_email'
		AND start_date LIKE '%2022-04-05%') AirCont

JOIN (SELECT max(INITIATIVE_CREATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_initiatives) Cont
		
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_EDW.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'GLOBAL_AUTHORS_EMAIL') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_EDW.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'GLOBAL_AUTHORS_EMAIL') mbs
  
  UNION

SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'ELOQUA' "SCHEMA"
,'GLOBAL_SOCIETY' "Table"
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

FROM (SELECT 
		TASK_ID
		,DAG_ID
		,START_DATE
		,END_DATE
		,DURATION
		,state
		
		FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
		
		WHERE DAG_ID IN ('eloqua_api_pipeline')
		AND task_id = 'load_t_s_global_society'
		AND start_date LIKE '%2022-04-05%') AirCont

JOIN (SELECT max(INITIATIVE_CREATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_initiatives) Cont
		
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_EDW.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'GLOBAL_SOCIETY') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_EDW.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'GLOBAL_SOCIETY') mbs
  
  UNION

SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'ELOQUA' "SCHEMA"
,'ONEWILEY_GLOB_PREFENCE' "Table"
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

FROM (SELECT 
		TASK_ID
		,DAG_ID
		,START_DATE
		,END_DATE
		,DURATION
		,state
		
		FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
		
		WHERE DAG_ID IN ('eloqua_api_pipeline')
		AND task_id = 'load_t_s_onewiley_glob_preference'
		AND start_date LIKE '%2022-04-05%') AirCont

JOIN (SELECT max(INITIATIVE_CREATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_initiatives) Cont
		
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_EDW.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'ONEWILEY_GLOB_PREFENCE') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_EDW.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'ONEWILEY_GLOB_PREFENCE') mbs
  
  UNION

SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'ELOQUA' "SCHEMA"
,'PAGEVIEW' "Table"
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

FROM (SELECT 
		TASK_ID
		,DAG_ID
		,START_DATE
		,END_DATE
		,DURATION
		,state
		
		FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
		
		WHERE DAG_ID IN ('eloqua_api_pipeline')
		AND task_id = 'load_t_s_pageview'
		AND start_date LIKE '%2022-04-05%') AirCont

JOIN (SELECT max(INITIATIVE_CREATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_initiatives) Cont
		
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_EDW.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'PAGEVIEW') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_EDW.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'PAGEVIEW') mbs
  
    UNION

SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'ELOQUA' "SCHEMA"
,'REJECTED_AUTHORS' "Table"
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

FROM (SELECT 
		TASK_ID
		,DAG_ID
		,START_DATE
		,END_DATE
		,DURATION
		,state
		
		FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
		
		WHERE DAG_ID IN ('eloqua_api_pipeline')
		AND task_id = 'load_t_s_rejected_authors'
		AND start_date LIKE '%2022-04-05%') AirCont

JOIN (SELECT max(INITIATIVE_CREATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_initiatives) Cont
		
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_EDW.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'REJECTED_AUTHORS') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_EDW.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'REJECTED_AUTHORS') mbs
  
    UNION

SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'ELOQUA' "SCHEMA"
,'SUBSCRIBE' "Table"
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

FROM (SELECT 
		TASK_ID
		,DAG_ID
		,START_DATE
		,END_DATE
		,DURATION
		,state
		
		FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
		
		WHERE DAG_ID IN ('eloqua_api_pipeline')
		AND task_id = 'load_t_s_subscribe'
		AND start_date LIKE '%2022-04-05%') AirCont

JOIN (SELECT max(INITIATIVE_CREATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_initiatives) Cont
		
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_EDW.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'SUBSCRIBE') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_EDW.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'SUBSCRIBE') mbs
  
    UNION

SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'ELOQUA' "SCHEMA"
,'UNSUBSCRIBE' "Table"
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

FROM (SELECT 
		TASK_ID
		,DAG_ID
		,START_DATE
		,END_DATE
		,DURATION
		,state
		
		FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
		
		WHERE DAG_ID IN ('eloqua_api_pipeline')
		AND task_id = 'load_t_s_unsubscribe'
		AND start_date LIKE '%2022-04-05%') AirCont

JOIN (SELECT max(INITIATIVE_CREATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_initiatives) Cont
		
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_EDW.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'UNSUBSCRIBE') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_EDW.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'UNSUBSCRIBE') mbs
  
    UNION

SELECT 
current_date() Report_date
,'PROD_EDW' "Warehouse"
,'ELOQUA' "SCHEMA"
,'WEBVISIT' "Table"
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

FROM (SELECT 
		TASK_ID
		,DAG_ID
		,START_DATE
		,END_DATE
		,DURATION
		,state
		
		FROM PROD_ODS.AIRFLOW_METADATA_MGMT.T_S_TASK_INSTANCE
		
		WHERE DAG_ID IN ('eloqua_api_pipeline')
		AND task_id = 'load_t_s_webvisit'
		AND start_date LIKE '%2022-04-05%') AirCont

JOIN (SELECT max(INITIATIVE_CREATED_AT) Last_modified
			,count(*) Row_size

		FROM PROD_EDW.KAPOST.T_S_initiatives) Cont
		
JOIN (SELECT count(COLUMN_NAME) cou
		FROM PROD_ODS.INFORMATION_SCHEMA."COLUMNS"
		WHERE TABLE_SCHEMA = 'ELOQUA"ELOQUA"'
		AND TABLE_NAME = 'WEBVISIT') col

JOIN (SELECT round(((bytes/1024)/1024),3) MBs
		FROM PROD_EDW.INFORMATION_SCHEMA."TABLES"
		WHERE TABLE_SCHEMA = 'ELOQUA'
		AND TABLE_NAME = 'WEBVISIT') mbs  		
);