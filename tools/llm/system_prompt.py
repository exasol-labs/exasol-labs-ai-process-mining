def save_system_prompt(env, project, prompt, connection):
    print("Saving System Prompt")

    with connection.connect() as con:

      sql = f"""
      UPDATE {env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.PROMPTS
      SET PROMPT = '{prompt}'
      WHERE PROJECT_ID = '{project}' 
      """
  
      con.execute(sql)