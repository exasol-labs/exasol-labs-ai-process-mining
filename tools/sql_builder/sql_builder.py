########################################################################################
## Helper function to create proper SQL statements based on various filter conditions ##
########################################################################################

def filter_to_sql(ms_exclude_steps: str, ms_include_steps: str, ms_meta_search_1: str) -> dict:

  exclude_steps = str(ms_exclude_steps.value).replace('[','(').replace(']', ')')
  include_steps = str(ms_include_steps.value).replace('[','(').replace(']', ')')
  meta_1_search = str(ms_meta_search_1.value).replace('[','(').replace(']', ')')
  
  if exclude_steps != '()':
      sql_where_exclude_steps = f"  STEP IN {exclude_steps} "
  else:
      sql_where_exclude_steps = " STEP IN ('START')"
  
  if include_steps != '()':
      sql_where_include_steps = f"  STEP IN {include_steps} "
  else:
      sql_where_include_steps = ''
  
  if meta_1_search != '()':
      sql_where_meta_1_search = f"  AND META_1 IN {meta_1_search} "
  else:
      sql_where_meta_1_search = ''
  
  WHERE_INC = ''
  WHERE_EXC = ''
  
  if sql_where_include_steps != '' or sql_where_meta_1_search != '':
      WHERE_INC = 'WHERE'
  
  if sql_where_exclude_steps != '' or sql_where_meta_1_search != '':
      WHERE_EXC = 'WHERE'


  rtn = {
    "where_exclude_steps": sql_where_exclude_steps,
    "where_include_steps": sql_where_include_steps,
    "where_meta_1_search": sql_where_meta_1_search,
    "where_exc": WHERE_EXC,
    "where_inc": WHERE_INC,
  }

  return rtn



#############################
## SQL Filtered Statistics ##
#############################

def sql_filtered_statistics_1(env, project: str, start_date, end_date, sql_parts) -> str:

  sql = f"""
  
    WITH TIMED_JOURNEYS AS (
        SELECT
        	*
        FROM
        	{env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.JOURNEYS
        WHERE
        	PROJECT_ID = '{project}' AND
            EVENT_TIME >= '{start_date}' AND EVENT_TIME <= '{end_date}' 	
    ),
    
    
    excluded_events AS (
        SELECT 
            DISTINCT EVENT_ID
        FROM 
            TIMED_JOURNEYS
        {sql_parts['where_exc']}
            {sql_parts['where_exclude_steps']}
            {sql_parts['where_meta_1_search']}
    ),
    
    included_events AS (
        SELECT 
            DISTINCT EVENT_ID
        FROM 
            TIMED_JOURNEYS
        {sql_parts['where_inc']} 
            {sql_parts['where_include_steps']} 
            {sql_parts['where_meta_1_search']}
    ),
    
    filtered_journeys AS (
        SELECT 
            *
        FROM 
            TIMED_JOURNEYS
        WHERE 
            EVENT_ID NOT IN (SELECT EVENT_ID FROM excluded_events) AND
            EVENT_ID IN (SELECT EVENT_ID FROM included_events)
    
    ),
    
    process_chains AS (
        SELECT
            EVENT_ID,
            EVENT_TIME AS FROM_TIME,
            STEP AS FROM_STEP,
            LEAD(STEP) OVER (PARTITION BY EVENT_ID ORDER BY EVENT_TIME) AS TO_STEP,
            LEAD(EVENT_TIME) OVER (PARTITION BY EVENT_ID ORDER BY EVENT_TIME) AS TO_TIME
        FROM 
            filtered_journeys
    ),
    
    durations AS (
        SELECT  
            EVENT_ID,
            MINUTES_BETWEEN(MAX(TO_TIME), MIN(FROM_TIME)) as duration_minutes
        FROM 
            process_chains
        WHERE 
            TO_STEP IS NOT NULL
        GROUP BY 
            EVENT_ID
    )
    
    
    SELECT 
        COUNT(DISTINCT EVENT_ID)               AS num_journeys,
        COUNT(EVENT_ID)                        AS num_steps,
        MIN(duration_minutes)                  AS min_duration_minutes,
        MEDIAN(duration_minutes)               AS median_duration_minutes,
        AVG(duration_minutes)                  AS avg_duration_minutes,
        MAX(duration_minutes)                  AS max_duration_minutes,
        STDDEV(duration_minutes)               AS stddev_duration_minutes
    FROM 
      durations;
  """

  return sql




def sql_filtered_statistics_2(env, project: str, start_date, end_date, sql_parts) -> str:

  sql = f"""
  
    WITH TIMED_JOURNEYS AS (
        SELECT
        	*
        FROM
        	{env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.JOURNEYS
        WHERE
        	PROJECT_ID = '{project}' AND
            EVENT_TIME >= '{start_date}' AND EVENT_TIME <= '{end_date}' 	
    ),
    
    
    excluded_events AS (
        SELECT 
            DISTINCT EVENT_ID
        FROM 
            TIMED_JOURNEYS
        {sql_parts['where_exc']}
            {sql_parts['where_exclude_steps']}
            {sql_parts['where_meta_1_search']}
    ),
    
    included_events AS (
        SELECT 
            DISTINCT EVENT_ID
        FROM 
            TIMED_JOURNEYS
        {sql_parts['where_inc']} 
            {sql_parts['where_include_steps']} 
            {sql_parts['where_meta_1_search']}
    ),
    
    filtered_journeys AS (
        SELECT 
            *
        FROM 
            TIMED_JOURNEYS
        WHERE 
            EVENT_ID NOT IN (SELECT EVENT_ID FROM excluded_events) AND
            EVENT_ID IN (SELECT EVENT_ID FROM included_events)
    
    )
    
    
    SELECT 
        COUNT(DISTINCT EVENT_ID)           AS num_journeys,
        COUNT(STEP)                        AS num_steps,
        COUNT(DISTINCT STEP)               AS num_distinct_steps
    FROM 
      filtered_journeys;
  """

  return sql