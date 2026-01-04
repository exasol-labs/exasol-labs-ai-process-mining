########################################################################################
## Helper function to create proper SQL statements based on various filter conditions ##
########################################################################################

def filter_to_sql(ms_exclude_steps: str, ms_include_steps: str, ms_meta_search_1: str, ms_meta_search_2: str, ms_meta_search_3: str) -> dict:

  exclude_steps = str(ms_exclude_steps.value).replace('[','(').replace(']', ')')
  include_steps = str(ms_include_steps.value).replace('[','(').replace(']', ')')
  
  if ms_meta_search_1 != '':
    meta_1_search = str(ms_meta_search_1.value).replace('[','(').replace(']', ')')
  else:
    meta_1_search = '()'
 
  if ms_meta_search_2 != '':
    meta_2_search = str(ms_meta_search_2.value).replace('[','(').replace(']', ')')
  else:
    meta_2_search = '()'
    
  if ms_meta_search_3 != '':
    meta_3_search = str(ms_meta_search_3.value).replace('[','(').replace(']', ')')
  else:
    meta_3_search = '()'
  
  if exclude_steps != '()':
      sql_where_exclude_steps = f"  STEP IN {exclude_steps} "
  else:
      sql_where_exclude_steps = " STEP IN ('START')"
  
  if include_steps != '()':
      sql_where_include_steps = f"  STEP IN {include_steps} "
  else:
      sql_where_include_steps = f"  STEP LIKE '%' "
  
  if meta_1_search != '()':
      sql_where_meta_1_search = f"  AND META_1 IN {meta_1_search} "
  else:
      sql_where_meta_1_search = f"  "

  if meta_2_search != '()':
      sql_where_meta_2_search = f"  AND META_2 IN {meta_2_search} "
  else:
      sql_where_meta_2_search = f"   "

  if meta_3_search != '()':
      sql_where_meta_3_search = f"  AND META_3 IN {meta_3_search} "
  else:
      sql_where_meta_3_search = f"   "
  
  WHERE_INC = ''
  WHERE_EXC = ''
  
  if sql_where_include_steps != '' or sql_where_meta_1_search != '' or sql_where_meta_2_search != '' or sql_where_meta_3_search != '':
      WHERE_INC = 'WHERE'
  
  if sql_where_exclude_steps != '' or sql_where_meta_1_search != '' or sql_where_meta_2_search != '' or sql_where_meta_3_search != '':
      WHERE_EXC = 'WHERE'


  rtn = {
    "where_exclude_steps": sql_where_exclude_steps,
    "where_include_steps": sql_where_include_steps,
    "where_meta_1_search": sql_where_meta_1_search,
    "where_meta_2_search": sql_where_meta_2_search,
    "where_meta_3_search": sql_where_meta_3_search,
    "where_exc": WHERE_EXC,
    "where_inc": WHERE_INC,
  }

  return rtn
  

#################################
## SQL Filtered Statistics - 1 ##
#################################

def sql_filtered_statistics_1(env, project: str, start_date, end_date, sql_parts) -> str:

  sql = f"""
  
    WITH TIMED_JOURNEYS AS (
        SELECT
        	*
        FROM
        	{env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.JOURNEYS
        WHERE
        	PROJECT_ID = '{project}' AND
            EVENT_TIME >= '{start_date} 00:00:00' AND EVENT_TIME <= '{end_date} 23:59:59' 	
            {sql_parts['where_meta_1_search']}
            {sql_parts['where_meta_2_search']}
            {sql_parts['where_meta_3_search']}
    ),
    
    
    excluded_events AS (
        SELECT 
            DISTINCT EVENT_ID
        FROM 
            TIMED_JOURNEYS
            {sql_parts['where_exc']}
            {sql_parts['where_exclude_steps']}        
    ),
    
    included_events AS (
        SELECT 
            DISTINCT EVENT_ID
        FROM 
            TIMED_JOURNEYS
            {sql_parts['where_inc']} 
            {sql_parts['where_include_steps']} 
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


#################################
## SQL Filtered Statistics - 2 ##
#################################

def sql_filtered_statistics_2(env, project: str, start_date, end_date, sql_parts) -> str:

  sql = f"""
  
    WITH TIMED_JOURNEYS AS (
        SELECT
        	*
        FROM
        	{env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.JOURNEYS
        WHERE
        	PROJECT_ID = '{project}' AND
            EVENT_TIME >= '{start_date} 00:00:00' AND EVENT_TIME <= '{end_date} 23:59:59' 	
            {sql_parts['where_meta_1_search']}
            {sql_parts['where_meta_2_search']}
            {sql_parts['where_meta_3_search']}
    ),
    
    
    excluded_events AS (
        SELECT 
            DISTINCT EVENT_ID
        FROM 
            TIMED_JOURNEYS
            {sql_parts['where_exc']}
            {sql_parts['where_exclude_steps']}
            
    ),
    
    included_events AS (
        SELECT 
            DISTINCT EVENT_ID
        FROM 
            TIMED_JOURNEYS
            {sql_parts['where_inc']} 
            {sql_parts['where_include_steps']} 
            
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


#####################################################
## Build the "Journey Table" for the Process Graph ##
#####################################################

def build_flowchart_structure(env, project, start_date, end_date, sql_parts):


    template_time = f"""
        TIMED_JOURNEYS AS (
            SELECT
                *
            FROM
                {env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.JOURNEYS
            WHERE
                PROJECT_ID = '{project}' AND
                EVENT_TIME >= '{start_date} 00:00:00' AND EVENT_TIME <= '{end_date} 23:59:59'
                {sql_parts['where_meta_1_search']}
                {sql_parts['where_meta_2_search']}
                {sql_parts['where_meta_3_search']}
        ),


    """


    template_exclude_1 = f"""
        EXC_EVENTS AS (
            SELECT
                TO_CHAR(CAST(EVENT_TIME AS DATE), 'YYYY-MM-DD') AS DATUM,
                EVENT_ID
            FROM
                TIMED_JOURNEYS
            WHERE
                {sql_parts['where_exclude_steps']}

            GROUP BY 
                local.DATUM, EVENT_ID
            ORDER BY
                local.DATUM
        ),
    """
    template_exclude_2 = f"""

    """

    template_include_1 = f"""
        INC_EVENTS AS (
            SELECT
                TO_CHAR(CAST(EVENT_TIME AS DATE), 'YYYY-MM-DD') AS DATUM,
                EVENT_ID
            FROM
                TIMED_JOURNEYS
            WHERE
                {sql_parts['where_include_steps']}

            GROUP BY 
                local.DATUM, EVENT_ID
            ORDER BY
                local.DATUM
            ),   
    """


    _sql = f"""
        WITH
            {template_time}
            {template_exclude_1}
            {template_include_1}

            FILTERED_JOURNEYS AS (
                SELECT 
                    J.*
                FROM 
                    TIMED_JOURNEYS J LEFT JOIN INC_EVENTS I ON j.EVENT_ID = I.EVENT_ID
                    LEFT JOIN EXC_EVENTS R ON  J.EVENT_ID   = R.EVENT_ID
                WHERE 
                    R.EVENT_ID IS NULL
                    AND (
                        I.EVENT_ID IS NOT NULL                    -- match event_id in VALID_EVENT_IDS
                        OR NOT EXISTS (SELECT 1 FROM INC_EVENTS)  -- table is empty → keep all
                      )
            ),

            PROCESS_CHAINS AS (
                SELECT  
                    J.EVENT_ID,
                    J.STEP AS FROM_STEP,
                    LEAD(J.STEP) OVER (PARTITION BY J.EVENT_ID ORDER BY J.EVENT_TIME) AS TO_STEP,
                    J.EVENT_TIME AS FROM_TIME,
                    LEAD(J.EVENT_TIME) OVER (PARTITION BY J.EVENT_ID ORDER BY J.EVENT_TIME) AS TO_TIME,
                    TO_CHAR(CAST(local.FROM_TIME AS DATE), 'YYYY-MM-DD') AS DATUM,
                    SECONDS_BETWEEN(local.TO_TIME, local.FROM_TIME) AS DURATION_SECONDS
                FROM 
                    FILTERED_JOURNEYS J
            )

            SELECT
                FROM_STEP,
                TO_STEP,
                COUNT(DISTINCT EVENT_ID),
                AVG(DURATION_SECONDS), 
                MIN(DURATION_SECONDS),
                MAX(DURATION_SECONDS),
                MEDIAN(DURATION_SECONDS),
                STDDEV(DURATION_SECONDS)
            FROM 
                PROCESS_CHAINS

            GROUP BY 
                FROM_STEP, TO_STEP
            HAVING 
                FROM_STEP <> TO_STEP
            ORDER BY 
                FROM_STEP

    """

    return _sql








  