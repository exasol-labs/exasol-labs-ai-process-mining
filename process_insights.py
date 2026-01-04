import marimo

__generated_with = "0.16.5"
app = marimo.App(
    width="full",
    app_title="Kea AI - Process Insights",
    auto_download=["html"],
    sql_output="polars",
)

with app.setup:
    # Initialization code that runs before all other cells

    import altair as alt
    import dotenv
    import json
    import marimo as mo
    import os
    import pandas as pd
    import polars as pl
    import pyexasol

    from collections import defaultdict
    from datetime import datetime, timedelta
    from dotenv import load_dotenv, dotenv_values
    from sqlalchemy import create_engine, sql, text

    from tools.llm.system_prompt import save_system_prompt
    from tools.sql_builder.sql_builder import filter_to_sql
    from tools.sql_builder.sql_builder import sql_filtered_statistics_1, sql_filtered_statistics_2, build_flowchart_structure
    from tools.statistics.statistic_widgets import filtered_statistics_widgets, total_statistics_widgets
    from tools.wrappers.text_wrappers import wrap_text, ResultText

    ## Debugging Flag

    DEBUG = True

    ## Menu constants

    MENU_PROCESS_FLOWCHART = 1
    MENU_AI_OVERVIEW = 2 
    MENU_A_B_FLOWCHARTS = 3
    MENU_INDIVIDUAL_FLOWCHART = 4
    MENU_GRAPHICAL_STATISTICS = 5
    MENU_SETTINGS = 6


    ## Get the environment

    env = dotenv.dotenv_values('.env')

    ## Certain States

    get_tab, set_tab = mo.state('')


    ## Create the Database Engine

    url = (
        f"exa+websocket://{env['KEA_PROCESS_INSIGHTS_EXA_DB_USER']}:{env['KEA_PROCESS_INSIGHTS_EXA_DB_PASSWORD']}@{env['KEA_PROCESS_INSIGHTS_EXA_DB_SERVER']}:{env['KEA_PROCESS_INSIGHTS_EXA_DB_PORT']}" \
        f"?schema={env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}&FINGERPRINT={env['KEA_PROCESS_INSIGHTS_EXA_DB_FINGERPRINT']}&CONNECTIONLCALL=en_US.UTF-8"
    )
    if DEBUG: 
        print(url)

    #Exasol_Database_Engine = create_engine(url, pool_timeout=3600)
    Exasol_Database_Engine = create_engine(url)


@app.cell
def sql__available_projects():
    list_available_projects = mo.sql(
        f"""
        SELECT
            PROJECT_ID,
            TITLE
        FROM
            {env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.PROJECTS
        ORDER BY
        	PROJECT_ID ASC
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (list_available_projects,)


@app.cell
def sql__meta_descriptions(dropdown_projects):
    meta_descriptions = mo.sql(
        f"""
        SELECT
        	META_1_TITLE    AS META_1,
            META_2_TITLE    AS META_2,
            META_3_TITLE    AS META_3
        FROM
            {env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.METAS
        WHERE
            PROJECT_ID = '{dropdown_projects.value}'
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (meta_descriptions,)


@app.cell
def _():
    ########################
    ## Filter Definitions ##
    ########################
    return


@app.cell
def filter__date_ranges(statistics_total):
    ##
    ## Building Input for Date Ranges
    ##

    min_date = ""
    max_date = ""

    min_date = str(statistics_total['earliest_eventdate'][0])
    max_date = str(statistics_total['most_recent_eventdate'][0])

    start_date_dt = datetime.strptime(min_date, "%Y-%m-%d")
    end_date_dt = datetime.strptime(max_date, "%Y-%m-%d")
    start_date_limited_dt = end_date_dt - timedelta(days=30)

    get_start_date, set_start_date = mo.state(start_date_limited_dt)
    get_end_date, set_end_date = mo.state(end_date_dt)

    ##
    ## Date Selection Boxes for single flowchart visualization
    ##

    start_date = mo.ui.date(
        #label="Start Date",
        value=get_start_date().strftime("%Y-%m-%d"),
        on_change=lambda x: set_start_date(pd.to_datetime(x))
    )

    end_date = mo.ui.date(
        #label="End Date",
        value=get_end_date().strftime("%Y-%m-%d"),
        on_change=lambda x: set_end_date(pd.to_datetime(x))
    )

    ##
    ## Date Selection Boxes for A/B flowchart visualization
    ##


    start_date_fc_a = mo.ui.date(
        #label="Start Date",
        value=get_start_date().strftime("%Y-%m-%d"),
        on_change=lambda x: set_start_date(pd.to_datetime(x))
    )

    end_date_fc_a = mo.ui.date(
        #label="End Date",
        value=get_end_date().strftime("%Y-%m-%d"),
        on_change=lambda x: set_end_date(pd.to_datetime(x))
    )

    start_date_fc_b = mo.ui.date(
        #label="Start Date",
        value=get_start_date().strftime("%Y-%m-%d"),
        on_change=lambda x: set_start_date(pd.to_datetime(x))
    )

    end_date_fc_b = mo.ui.date(
        #label="End Date",
        value=get_end_date().strftime("%Y-%m-%d"),
        on_change=lambda x: set_end_date(pd.to_datetime(x))
    )
    return (
        end_date,
        end_date_fc_a,
        end_date_fc_b,
        start_date,
        start_date_fc_a,
        start_date_fc_b,
    )


@app.cell
def filter__metric_selection():
    _metrics_pt = ["Number of Journeys", "Average Transition Time", "Minimum Transition Time", "Median Transition Time", "StdDev Transition Time", "Maximum Transition Time"]
    _metrics_a  = ["Number of Journeys", "Average Transition Time", "Minimum Transition Time", "Median Transition Time", "StdDev Transition Time", "Maximum Transition Time"]
    _metrics_b  = ["Number of Journeys", "Average Transition Time", "Minimum Transition Time", "Median Transition Time", "StdDev Transition Time", "Maximum Transition Time"]

    metric_selection_pt = mo.ui.multiselect(options=_metrics_pt, label='Metric ', value=['Number of Journeys'], full_width=True, max_selections=1)
    metric_selection_a  = mo.ui.multiselect(options=_metrics_a,  label='Metric ', value=['Number of Journeys'], full_width=True, max_selections=1)
    metric_selection_b  = mo.ui.multiselect(options=_metrics_b,  label='Metric ', value=['Number of Journeys'], full_width=True, max_selections=1)
    return metric_selection_a, metric_selection_b, metric_selection_pt


@app.cell
def filter__meta_search(
    list_journey_metas_1,
    list_journey_metas_2,
    list_journey_metas_3,
    meta_descriptions,
):
    if meta_descriptions['meta_1'][0]:
        ms_meta_search_1 = mo.ui.multiselect.from_series(list_journey_metas_1["meta_1"], label=f"Select {meta_descriptions['meta_1'][0]}", full_width=True)
        ms_meta_search_1_fc_a = mo.ui.multiselect.from_series(list_journey_metas_1["meta_1"], label=f"Select {meta_descriptions['meta_1'][0]}", full_width=True)
        ms_meta_search_1_fc_b = mo.ui.multiselect.from_series(list_journey_metas_1["meta_1"], label=f"Select {meta_descriptions['meta_1'][0]}", full_width=True)
    else:
        ms_meta_search_1 = ''
        ms_meta_search_1_fc_a = ''
        ms_meta_search_1_fc_b = ''

    if meta_descriptions['meta_2'][0]:
        ms_meta_search_2 = mo.ui.multiselect.from_series(list_journey_metas_2["meta_2"], label=f"Select {meta_descriptions['meta_2'][0]}", full_width=True)
        ms_meta_search_2_fc_a = mo.ui.multiselect.from_series(list_journey_metas_2["meta_2"], label=f"Select {meta_descriptions['meta_2'][0]}", full_width=True)
        ms_meta_search_2_fc_b = mo.ui.multiselect.from_series(list_journey_metas_2["meta_2"], label=f"Select {meta_descriptions['meta_2'][0]}", full_width=True)
    else:
        ms_meta_search_2 = ''
        ms_meta_search_2_fc_a = ''    
        ms_meta_search_2_fc_b = ''

    if meta_descriptions['meta_3'][0]:
        ms_meta_search_3 = mo.ui.multiselect.from_series(list_journey_metas_3["meta_3"], label=f"Select {meta_descriptions['meta_3'][0]}", full_width=True)
        ms_meta_search_3_fc_a = mo.ui.multiselect.from_series(list_journey_metas_3["meta_3"], label=f"Select {meta_descriptions['meta_3'][0]}", full_width=True)
        ms_meta_search_3_fc_b = mo.ui.multiselect.from_series(list_journey_metas_3["meta_3"], label=f"Select {meta_descriptions['meta_3'][0]}", full_width=True)
    else:
        ms_meta_search_3 = ''
        ms_meta_search_3_fc_a = ''
        ms_meta_search_3_fc_b = ''
    return (
        ms_meta_search_1,
        ms_meta_search_1_fc_a,
        ms_meta_search_1_fc_b,
        ms_meta_search_2,
        ms_meta_search_2_fc_a,
        ms_meta_search_2_fc_b,
        ms_meta_search_3,
        ms_meta_search_3_fc_a,
        ms_meta_search_3_fc_b,
    )


@app.cell
def sql__list_metas(dropdown_projects):
    list_journey_metas_1 = mo.sql(
        f"""
        SELECT
        	DISTINCT META_1
        FROM
        	{env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.JOURNEYS
        WHERE
            PROJECT_ID = '{dropdown_projects.value}'
        ORDER BY META_1 ASC
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (list_journey_metas_1,)


@app.cell
def _(dropdown_projects):
    list_journey_metas_2 = mo.sql(
        f"""
        SELECT
        	DISTINCT META_2
        FROM
        	{env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.JOURNEYS
        WHERE
            PROJECT_ID = '{dropdown_projects.value}'
        ORDER BY META_2 ASC
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (list_journey_metas_2,)


@app.cell
def _(dropdown_projects):
    list_journey_metas_3 = mo.sql(
        f"""
        SELECT
        	DISTINCT META_3
        FROM
        	{env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.JOURNEYS
        WHERE
            PROJECT_ID = '{dropdown_projects.value}'
        ORDER BY META_3 ASC
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (list_journey_metas_3,)


@app.cell
def dropdown__projects_list(list_available_projects):
    # Convert to a dictionary: TITLE as key, PROJECT_ID as value (adjust if needed)

    projects_dict = dict(list_available_projects.select(["title", "project_id"]).iter_rows())

    if DEBUG: print(projects_dict)

    # Get the first TITLE (key) and its corresponding PROJECT_ID (value)

    first_title = list(projects_dict.keys())[0]
    selected = projects_dict[first_title]

    if DEBUG: 
        print(f"Selected: {first_title} => {selected}")

    selected = first_title


    dropdown_projects = mo.ui.dropdown(options=projects_dict, 
                                       label="Select a project", 
                                       searchable=True, 
                                       value=selected)
    return (dropdown_projects,)


@app.cell(hide_code=True)
def _(dropdown_projects):
    mo.md(
        f"""
    # {dropdown_projects.selected_key}
    </br>
    """
    )
    return


@app.cell(hide_code=True)
def title__working_project(dropdown_projects):
    ## Date Range Selection

    sql_min_max_datetime = f"""
    SELECT 
        MIN(TO_DATE(EVENT_TIME, 'DD-MM-YYYY')) AS min_datetime,
        MAX(TO_DATE(EVENT_TIME, 'DD-MM-YYYY')) AS max_datetime
    FROM 
        JOURNEYS
    WHERE
        PROJECT_ID = '{dropdown_projects.value}'
    """

    ## Get steps

    sql_get_steps = f"""
    SELECT
        STEP
    FROM
        STEPS
    WHERE
        PROJECT_ID = '{dropdown_projects.value}'
    """

    sql_get_metas = f"""
    SELECT
        DISTINCT META_1
    FROM
        JOURNEYS
    WHERE
        PROJECT_ID = '{dropdown_projects.value}'
    """
    return


@app.cell
def sql__num_statistics(dropdown_projects):
    statistics_num_steps = mo.sql(
        f"""
        SELECT 
            COUNT(STEP)          AS total_steps,
            COUNT(DISTINCT STEP) AS distinct_steps
        FROM 
            {env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.JOURNEYS
        WHERE
            PROJECT_ID = '{dropdown_projects.value}'
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (statistics_num_steps,)


@app.cell
def sql__unique_steps(dropdown_projects):
    list_available_steps = mo.sql(
        f"""
        SELECT 
            DISTINCT STEP
        FROM
            {env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.STEPS
        WHERE
            PROJECT_ID = '{dropdown_projects.value}'
        ORDER BY
        	STEP ASC
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (list_available_steps,)


@app.cell
def _():
    switch_visualization_type = mo.ui.switch(label='Flowchart / Sankey Diagram')
    return


@app.cell
def switch__flowchart_orienttion():
    switch_flowchart_orientation = mo.ui.switch(label="Orientation Flowchart: Top Down / Left-Right")
    return (switch_flowchart_orientation,)


@app.cell
def _():
    ## Hier kommt die Switch-Steuerung hin!
    return


@app.cell
def ui__filter_groups(
    end_date,
    end_date_fc_a,
    end_date_fc_b,
    metric_selection_a,
    metric_selection_b,
    metric_selection_pt,
    ms_exclude_steps_fc_a,
    ms_exclude_steps_fc_b,
    ms_exclude_steps_pt,
    ms_include_steps_fc_a,
    ms_include_steps_fc_b,
    ms_include_steps_pt,
    ms_meta_search_1,
    ms_meta_search_1_fc_a,
    ms_meta_search_1_fc_b,
    ms_meta_search_2,
    ms_meta_search_2_fc_a,
    ms_meta_search_2_fc_b,
    ms_meta_search_3,
    ms_meta_search_3_fc_a,
    ms_meta_search_3_fc_b,
    start_date,
    start_date_fc_a,
    start_date_fc_b,
    switch_flowchart_orientation,
):

    filter_process_tree = mo.vstack([
                    #mo.hstack([switch_visualization_type]),
                    mo.hstack([switch_flowchart_orientation]),
                    mo.hstack([start_date, mo.md("->"), end_date], gap=0.1, justify="start"),
                    mo.vstack([
                        metric_selection_pt,
                        ms_include_steps_pt, 
                        ms_exclude_steps_pt,
                        ms_meta_search_1,
                        ms_meta_search_2,
                        ms_meta_search_3,
                    ]),
                ],
                gap=2,
                heights=[0,0,0,0],
                )

    individual_journey_input_id = mo.ui.text(placeholder='Search for unique ID...', label='Journey-ID', full_width=True)
    filter_individual_journey = mo.vstack([
                                    mo.hstack([switch_flowchart_orientation]),
                                    mo.hstack([individual_journey_input_id], widths=[1])
                                ],
                                gap=2,
                                heights=[0,0]
                                )

    filter_process_tree_a = mo.vstack([

                    mo.hstack([mo.md("### Filtergroup -A-")]),
                    mo.hstack([start_date_fc_a, mo.md("->"), end_date_fc_a], gap=0.1, justify="start"),
                    mo.vstack([
                        metric_selection_a,
                        ms_include_steps_fc_a, 
                        ms_exclude_steps_fc_a,
                        ms_meta_search_1_fc_a,
                        ms_meta_search_2_fc_a,
                        ms_meta_search_3_fc_a,
                    ]),
                ],
                gap=2,
                #heights=[0,1],
                )

    filter_process_tree_b = mo.vstack([
                    mo.hstack([mo.md("### Filtergroup -B-")]),
                    mo.hstack([start_date_fc_b, mo.md("->"), end_date_fc_b], gap=0.1, justify="start"),
                    mo.vstack([
                        metric_selection_b,
                        ms_include_steps_fc_b, 
                        ms_exclude_steps_fc_b,
                        ms_meta_search_1_fc_b,
                        ms_meta_search_2_fc_b,
                        ms_meta_search_3_fc_b,
                    ]),
                ],
                gap=2,
                heights=[0,0,0],
                )

    filter_ab = mo.vstack([filter_process_tree_a, filter_process_tree_b], gap=2, heights=[0,1])

    filter_se = mo.hstack([])

    default_temp = 0.75
    slider_temperature_llm = mo.ui.slider(label='Temperature for LLM', start=0, stop=1, step=0.05, full_width=True, show_value=True, value=default_temp)
    ai_button = mo.ui.run_button(label='AI based Analysis of Process Tree',  full_width=True) 
    filter_group_ai = mo.vstack([
                        mo.hstack([ai_button ]),
                        mo.hstack([mo.md("")]),
                        mo.hstack([slider_temperature_llm]),

                      ],
                      gap=2,
                      heights=[0,0,0],
                        )
    return (
        ai_button,
        filter_ab,
        filter_group_ai,
        filter_individual_journey,
        filter_process_tree,
        filter_se,
        individual_journey_input_id,
        slider_temperature_llm,
    )


@app.cell
def visual__sidebar(dropdown_projects, filter_group):
    mo.sidebar(
        [
            mo.md('# _Kea AI - Process Insights_'),
            mo.md('#### _Process Insights at your fingertips_'),
            mo.md('</br>'),

            dropdown_projects,

            mo.md("</br>"),
            mo.md("##Filters and Actions"),
            mo.md("</br>"),

            filter_group,

        ],
     width="17%"
    )
    return


@app.cell
def _():
    ################
    ## Statistics ##
    ################
    return


@app.cell
def _(statistics_num_steps, statistics_total):
    total_statistics_1 = total_statistics_widgets(statistics_total, statistics_num_steps)['total_statistics_1']
    total_statistics_2 = total_statistics_widgets(statistics_total, statistics_num_steps)['total_statistics_2']
    return total_statistics_1, total_statistics_2


@app.cell
def _(
    dataframe_statistics_filtered_pt_1,
    dataframe_statistics_filtered_pt_2,
    end_date,
    start_date,
):
    filtered_statistics_pt_1 = filtered_statistics_widgets(dataframe_statistics_filtered_pt_1, dataframe_statistics_filtered_pt_2, start_date.value, end_date.value)['filtered_statistics_1']
    filtered_statistics_pt_2 = filtered_statistics_widgets(dataframe_statistics_filtered_pt_1, dataframe_statistics_filtered_pt_2, start_date.value, end_date.value)['filtered_statistics_2']
    return filtered_statistics_pt_1, filtered_statistics_pt_2


@app.cell
def _(
    dataframe_statistics_filtered_a_1,
    dataframe_statistics_filtered_a_2,
    end_date_fc_a,
    start_date_fc_a,
):
    filtered_statistics_widgets_a_1 = filtered_statistics_widgets(dataframe_statistics_filtered_a_1, dataframe_statistics_filtered_a_2, start_date_fc_a.value, end_date_fc_a.value)['filtered_statistics_1']
    filtered_statistics_widgets_a_2 = filtered_statistics_widgets(dataframe_statistics_filtered_a_1, dataframe_statistics_filtered_a_2, start_date_fc_a.value, end_date_fc_a.value)['filtered_statistics_2']
    return filtered_statistics_widgets_a_1, filtered_statistics_widgets_a_2


@app.cell
def _(
    dataframe_statistics_filtered_b_1,
    dataframe_statistics_filtered_b_2,
    end_date_fc_b,
    start_date_fc_b,
):
    filtered_statistics_widgets_b_1 = filtered_statistics_widgets(dataframe_statistics_filtered_b_1, dataframe_statistics_filtered_b_2, start_date_fc_b.value, end_date_fc_b.value)['filtered_statistics_1']
    filtered_statistics_widgets_b_2 = filtered_statistics_widgets(dataframe_statistics_filtered_b_1, dataframe_statistics_filtered_b_2, start_date_fc_b.value, end_date_fc_b.value)['filtered_statistics_2']
    return filtered_statistics_widgets_b_1, filtered_statistics_widgets_b_2


@app.cell
def _(
    filtered_statistics_pt_1,
    filtered_statistics_pt_2,
    filtered_statistics_widgets_a_1,
    filtered_statistics_widgets_a_2,
    filtered_statistics_widgets_b_1,
    filtered_statistics_widgets_b_2,
    individual_journey_input_id,
    individual_journey_statistics,
    total_statistics_1,
    total_statistics_2,
):
    statistics_to = mo.accordion({
            '### Statistics for all Processes': mo.vstack([ total_statistics_1, total_statistics_2 ])
            },
            multiple = True
        )

    statistics_pt = mo.accordion({
            '### Statistics for all Processes': mo.vstack([ total_statistics_1, total_statistics_2 ]),
            '### Statistics for Filter-Settings': mo.vstack([ filtered_statistics_pt_1, filtered_statistics_pt_2 ]),
            },
            multiple = True
        )

    statistics_ab = mo.accordion({
            '### Statistics for all Processes': mo.vstack([ total_statistics_1, total_statistics_2 ]),
            '### Statistics for Filter-Settings - Process-Tree A': mo.vstack([ filtered_statistics_widgets_a_1, filtered_statistics_widgets_a_2 ]),
            '### Statistics for Filter-Settings - Process-Tree B': mo.vstack([ filtered_statistics_widgets_b_1, filtered_statistics_widgets_b_2 ]),
            },
            multiple = True
        )  

    if individual_journey_input_id.value != '':
        statistics_ij = mo.accordion({
                '### Statistics for all Processes': mo.vstack([ total_statistics_1, total_statistics_2 ]),
                '### Statistics for the individual Journey': mo.vstack([ individual_journey_statistics ])
                },
                multiple = True
            )

    statistics_se = mo.hstack([mo.md('<br/><br/>')])
    return (
        statistics_ab,
        statistics_ij,
        statistics_pt,
        statistics_se,
        statistics_to,
    )


@app.cell
def _(button_ab, button_ai, button_gs, button_ij, button_pt, button_se):
    button_row = mo.hstack([button_pt, button_ai, button_ab, button_ij, button_gs, button_se], justify='center', gap=0.25)
    return (button_row,)


@app.cell
def _(button_row):
    button_row
    return


@app.cell
def _():
    mo.md(r"""<br/>""")
    return


@app.cell
def _(statistics):
    statistics
    return


@app.function
# get_tab, set_tab = mo.state('')

def set_menu(menu: str) -> str:

    print(menu)
    print("get_tab():", get_tab())

    if get_tab() == '':
        set_tab('Process - Tree')

    menu_map = {
        'Process - Tree': MENU_PROCESS_FLOWCHART,                    # 1
        'Overview by AI': MENU_AI_OVERVIEW,                          # 2
        'A/B Comparison': MENU_A_B_FLOWCHARTS,                       # 3
        'Individual Journey Inspection': MENU_INDIVIDUAL_FLOWCHART,  # 4
        'Selected Statistics': MENU_GRAPHICAL_STATISTICS,            # 5
        'Settings': MENU_SETTINGS,                                   # 6       
    }

    #set_tab(menu_map.get(menu))

    res = menu_map.get(menu)

    return res


@app.cell
def _(mermaid_diagram, ms_exclude_steps_pt, ms_include_steps_pt):
    ##
    ## Tab Menu Items
    ##

    tab_content_process_tree_total = mo.hstack([mo.vstack([
                                     mo.md("<br/>"),
                                     mo.hstack([mo.md(f"**Including Steps**: {ms_include_steps_pt.value}"), mo.md(f"**Excluding Steps**: {ms_exclude_steps_pt.value}")], widths=[1,1]),
                                     mo.md("<br/><br/>"),
                                     mo.mermaid(mermaid_diagram).style(width="150%", height="150%").center(),
                                   ])]),
    return


@app.cell
def _(
    individual_journey_flowchart,
    llm_result_single_flowchart,
    mermaid_diagram,
    mermaid_diagram_fc_a,
    mermaid_diagram_fc_b,
    ms_exclude_steps_fc_a,
    ms_exclude_steps_fc_b,
    ms_exclude_steps_pt,
    ms_include_steps_fc_a,
    ms_include_steps_fc_b,
    ms_include_steps_pt,
    settings,
    statistics_row_1,
    statistics_row_2,
):
    tab_pt = mo.hstack([mo.vstack([
                                  mo.md("<br/>"),
                                  mo.hstack([mo.md(f"**Including Steps**: {ms_include_steps_pt.value}"), mo.md(f"**Excluding Steps**: {ms_exclude_steps_pt.value}")], widths=[1,1]),
                                  mo.md("<br/><br/>"),
                                  mo.mermaid(mermaid_diagram).style(width="150%", height="150%").center(),
                              ])])

    tab_ai = mo.vstack([
                                  mo.md("<br/>"),
                                  mo.hstack([mo.md(f"**Including Steps**: {ms_include_steps_pt.value}"), mo.md(f"**Excluding Steps**: {ms_exclude_steps_pt.value}")], widths=[1,1]),
                                  mo.md("<br/><br/>"),
                                  mo.md(str(llm_result_single_flowchart)),                                  
                                ])

    tab_ab = mo.hstack([
                 mo.vstack([
                     mo.md("<br/><br/>"),
                     mo.hstack([mo.md(f"**Including Steps**: {ms_include_steps_fc_a.value}"), mo.md(f"**Excluding Steps**: {ms_exclude_steps_fc_a.value}")], widths=[1,1]),
                     mo.md("<br/><br/>"),                                    
                     mo.mermaid(mermaid_diagram_fc_a).style(width="150%", height="150%").center(),
                 ]),           
                 mo.vstack([
                     mo.md("<br/><br/>"),
                     mo.hstack([mo.md(f"**Including Steps**: {ms_include_steps_fc_b.value}"), mo.md(f"**Excluding Steps**: {ms_exclude_steps_fc_b.value}")], widths=[1,1]),
                     mo.md("<br/><br/>"),                                    
                     mo.mermaid(mermaid_diagram_fc_b).style(width="150%", height="150%").center(),
                 ]),
             ],
             widths = [1,1],
             gap = 5.0,
             )

    tab_ij = mo.vstack([mo.md("</br>"), mo.mermaid(individual_journey_flowchart).style(width="100%").center()])

    tab_gs = mo.vstack([mo.md("</br>"), 
                                              statistics_row_1, 
                                              statistics_row_2,
                                              mo.hstack([mo.md(f"**Including Steps**: {ms_include_steps_pt.value}"), mo.md(f"**Excluding Steps**: {ms_exclude_steps_pt.value}")], widths=[1,1], align="stretch"),
                                             ])
    tab_se = mo.hstack([settings])
    return tab_ab, tab_ai, tab_gs, tab_ij, tab_pt, tab_se


@app.cell
def _():
    return


@app.cell
def _(xxx):
    kind_pt = kind_ai = kind_ab = kind_ij = kind_gs = kind_se = 'neutral'


    def set_tab_color(sel_tab: str):

        global kind_pt, kind_ai, kind_ab, kind_ij, kind_gs, kind_se
        global xxx

        kind_pt = kind_ai = kind_ab = kind_ij = kind_gs = kind_se = 'neutral'

        if sel_tab == 'pt':
            kind_pt = 'neutral'
        elif sel_tab == 'ai':
            kind_ai = 'neutral'
        elif sel_tab == 'ab':
            kind_ab = 'neutral'
        elif sel_tab == 'ij':
            kind_ij = 'neutral'
        elif sel_tab == 'gs':
            kind_gs = 'neutral'
        elif sel_tab == 'se':
            kind_se = 'neutral'
    return kind_ab, kind_ai, kind_gs, kind_ij, kind_pt, kind_se, set_tab_color


@app.cell
def _(set_tab_color):
    set_tab_color('pt')
    return


@app.cell
def gui__menu_buttons(kind_ab, kind_ai, kind_gs, kind_ij, kind_pt, kind_se):

    button_pt = mo.ui.run_button(label='Process Tree',full_width=True, kind = kind_pt)
    button_ai = mo.ui.run_button(label='AI-Generated Overview', full_width=True, kind=kind_ai)
    button_ab = mo.ui.run_button(label='A/B Comparison', full_width=True, kind=kind_ab)
    button_ij = mo.ui.run_button(label='Individual Journey inspection',full_width=True, kind=kind_ij)
    button_gs = mo.ui.run_button(label='Graphical Statistics', full_width=True, kind=kind_gs)
    button_se = mo.ui.run_button(label='Settings', full_width=True, kind=kind_se)
    return button_ab, button_ai, button_gs, button_ij, button_pt, button_se


@app.cell
def _(
    button_ab,
    button_ai,
    button_gs,
    button_ij,
    button_pt,
    button_se,
    filter_ab,
    filter_group_ai,
    filter_individual_journey,
    filter_process_tree,
    filter_se,
    individual_journey_input_id,
    set_tab_color,
    statistics_ab,
    statistics_ij,
    statistics_pt,
    statistics_se,
    statistics_to,
    tab_ab,
    tab_ai,
    tab_gs,
    tab_ij,
    tab_pt,
    tab_se,
):
    statistics = statistics_pt

    button_map = [
        (button_pt, tab_pt, filter_process_tree, 'pt'),
        (button_ai, tab_ai, filter_group_ai, 'ai'),
        (button_ab, tab_ab, filter_ab, 'ab'),
        (button_ij, tab_ij, filter_individual_journey, 'ij'),
        (button_gs, tab_gs, filter_process_tree, 'gs'),
        (button_se, tab_se, filter_se, 'se'),
    ]

    viewer = button_map[0][1]
    filter_group = button_map[0][2]

    for button, tab, filter_group_value, idx in button_map:
        if button.value:
            viewer = tab
            set_tab_color(idx)
            if filter_group_value is not None:
                filter_group = filter_group_value

            if idx == 'pt' or idx == 'ai':
                statistics = statistics_pt
            elif idx == 'ab':
                statistics=statistics_ab
            elif idx == 'ij' and individual_journey_input_id.value != '':
                statistics = statistics_ij
            elif idx == 'gs':
                statistics = statistics_to
            elif idx == 'se':
                statistics = statistics_se

            break
    return filter_group, statistics, viewer


@app.cell
def _(viewer):
    viewer
    return


@app.cell
def _():
    #menu_selected = set_menu(tabs.value)
    #set_tab(tabs.value)
    #print("Result: ", menu_selected)
    #print("tabs-value: ", tabs.value)
    return


@app.cell
def _():
    return


@app.cell
def _(dropdown_projects):


    sql_completeness_statistics = f"""
    WITH FINISHED_PROCESSES AS (
              SELECT
                        COUNT(DISTINCT J.EVENT_ID) AS FINISHED_PROCS
              FROM
                        JOURNEYS J JOIN STEPS S ON (J.STEP_ID = S.STEP_ID)
              WHERE 
                        S.END_OF_PROCESS = '1'
                        AND J.PROJECT_ID = '{dropdown_projects.value}'
              ),

    TOTAL_PROCESSES AS (
              SELECT 
                        COUNT (DISTINCT EVENT_ID)  AS TOTAL_PROCS 
              FROM 
                        JOURNEYS J
              WHERE 
                        J.PROJECT_ID = '{dropdown_projects.value}'
              )

    SELECT (TOTAL_PROCS-FINISHED_PROCS) AS INCOMPLETE_PROCS
    FROM FINISHED_PROCESSES, TOTAL_PROCESSES

    """


    sql_last_event_started = f"""
    SELECT MAX(EVENT_TIME) FROM JOURNEYS LIMIT 1
    """
    return


@app.cell
def _():
    ##########################################
    ## Create Statistics when filter change ##
    ##########################################
    return


@app.cell
def _(
    ms_exclude_steps_pt,
    ms_include_steps_pt,
    ms_meta_search_1,
    ms_meta_search_2,
    ms_meta_search_3,
):
    sql_parts_pt = filter_to_sql(ms_exclude_steps_pt, ms_include_steps_pt, ms_meta_search_1, ms_meta_search_2, ms_meta_search_3)
    return (sql_parts_pt,)


@app.cell
def _(dropdown_projects, end_date, sql_parts_pt, start_date):
    sql_filtered_statistics_pt_1 = sql_filtered_statistics_1(env=env, project=dropdown_projects.value, start_date=start_date.value, end_date=end_date.value, sql_parts=sql_parts_pt)
    sql_filtered_statistics_pt_2 = sql_filtered_statistics_2(env=env, project=dropdown_projects.value, start_date=start_date.value, end_date=end_date.value, sql_parts=sql_parts_pt)
    return sql_filtered_statistics_pt_1, sql_filtered_statistics_pt_2


@app.cell
def _(
    dataframe_statistics_filtered_pt_1,
    dataframe_statistics_filtered_pt_2,
    end_date,
    start_date,
):
    filtered_statistics_widgets_pt_1 = filtered_statistics_widgets(dataframe_statistics_filtered_pt_1, dataframe_statistics_filtered_pt_2, start_date.value, end_date.value)
    return


@app.cell
def _(
    ms_exclude_steps_fc_a,
    ms_include_steps_fc_a,
    ms_meta_search_1_fc_a,
    ms_meta_search_2_fc_a,
    ms_meta_search_3_fc_a,
):
    sql_parts_a = filter_to_sql(ms_exclude_steps_fc_a, ms_include_steps_fc_a, ms_meta_search_1_fc_a, ms_meta_search_2_fc_a, ms_meta_search_3_fc_a)
    return (sql_parts_a,)


@app.cell
def _(dropdown_projects, end_date_fc_a, sql_parts_a, start_date_fc_a):
    sql_filtered_statistics_a_1  = sql_filtered_statistics_1(env=env, project=dropdown_projects.value, start_date=start_date_fc_a.value, end_date=end_date_fc_a.value, sql_parts=sql_parts_a)
    sql_filtered_statistics_a_2  = sql_filtered_statistics_2(env=env, project=dropdown_projects.value, start_date=start_date_fc_a.value, end_date=end_date_fc_a.value, sql_parts=sql_parts_a)
    return sql_filtered_statistics_a_1, sql_filtered_statistics_a_2


@app.cell
def _(
    ms_exclude_steps_fc_b,
    ms_include_steps_fc_b,
    ms_meta_search_1_fc_b,
    ms_meta_search_2_fc_b,
    ms_meta_search_3_fc_b,
):
    sql_parts_b = filter_to_sql(ms_exclude_steps_fc_b, ms_include_steps_fc_b, ms_meta_search_1_fc_b, ms_meta_search_2_fc_b, ms_meta_search_3_fc_b)
    return (sql_parts_b,)


@app.cell
def _(dropdown_projects, end_date_fc_b, sql_parts_b, start_date_fc_b):
    sql_filtered_statistics_b_1  = sql_filtered_statistics_1(env=env, project=dropdown_projects.value, start_date=start_date_fc_b.value, end_date=end_date_fc_b.value, sql_parts=sql_parts_b)
    sql_filtered_statistics_b_2  = sql_filtered_statistics_2(env=env, project=dropdown_projects.value, start_date=start_date_fc_b.value, end_date=end_date_fc_b.value, sql_parts=sql_parts_b)
    return sql_filtered_statistics_b_1, sql_filtered_statistics_b_2


@app.cell
def _(list_available_steps):
    from tools.inputs.filters import handle_inc
    from tools.inputs.filters import handle_exc
    from tools.inputs.filters import get_state_inc, set_state_inc, get_state_exc, set_state_exc

    options_list = list_available_steps["step"]
    return get_state_exc, get_state_inc, handle_exc, handle_inc, options_list


@app.cell
def sql__statistics_total(dropdown_projects):
    statistics_total = mo.sql(
        f"""
        SELECT
            COUNT(*)                               AS num_journeys,
            MIN(duration_minutes)                  AS min_duration_minutes,
            MEDIAN(duration_minutes)               AS median_duration_minutes,
            AVG(duration_minutes)                  AS avg_duration_minutes,
            MAX(duration_minutes)                  AS max_duration_minutes,
            STDDEV(duration_minutes)               AS stddev_duration_minutes,
            MIN(min_datetime)                      AS earliest_eventdate,
            MAX(max_datetime)                      AS most_recent_eventdate
        FROM (
            SELECT 
                EVENT_ID,
                MIN(TO_DATE(EVENT_TIME, 'DD-MM-YYYY')) AS min_datetime,
                MAX(TO_DATE(EVENT_TIME, 'DD-MM-YYYY')) AS max_datetime,
                MINUTES_BETWEEN(MAX(EVENT_TIME), MIN(EVENT_TIME)) AS duration_minutes
            FROM 
            	{env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.JOURNEYS
            WHERE 
            	PROJECT_ID = '{dropdown_projects.value}'
            GROUP BY 
            	EVENT_ID
        ) AS durations;
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (statistics_total,)


@app.cell
def sql__filtered_statistics_pt_1(sql_filtered_statistics_pt_1):
    dataframe_statistics_filtered_pt_1 = mo.sql(
        f"""
        {sql_filtered_statistics_pt_1}
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (dataframe_statistics_filtered_pt_1,)


@app.cell
def sql__filtered_statistics_pt_2(sql_filtered_statistics_pt_2):
    dataframe_statistics_filtered_pt_2 = mo.sql(
        f"""
        {sql_filtered_statistics_pt_2}
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (dataframe_statistics_filtered_pt_2,)


@app.cell
def sql__filtered_statistics_1_1(sql_filtered_statistics_a_1):
    dataframe_statistics_filtered_a_1 = mo.sql(
        f"""
        {sql_filtered_statistics_a_1}
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (dataframe_statistics_filtered_a_1,)


@app.cell
def sql__filtered_statistics_a_2(sql_filtered_statistics_a_2):
    dataframe_statistics_filtered_a_2 = mo.sql(
        f"""
        {sql_filtered_statistics_a_2}
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (dataframe_statistics_filtered_a_2,)


@app.cell
def sql__filtered_statistics_b_1(sql_filtered_statistics_b_1):
    dataframe_statistics_filtered_b_1 = mo.sql(
        f"""
        {sql_filtered_statistics_b_1}
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (dataframe_statistics_filtered_b_1,)


@app.cell
def sql__filtered_statistics_b_2(sql_filtered_statistics_b_2):
    dataframe_statistics_filtered_b_2 = mo.sql(
        f"""
        {sql_filtered_statistics_b_2}
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (dataframe_statistics_filtered_b_2,)


@app.cell
def _():
    ####################################################
    ## Defining the Exclude/Include Filter Selections ##
    ####################################################
    return


@app.cell
def _():
    return


@app.cell
def _(get_state_inc, handle_inc, options_list):
    # Create the multiselect components

    ms_include_steps_pt = mo.ui.multiselect(options=options_list, label="Include Steps", value=get_state_inc(), full_width=True, on_change=handle_inc)
    return (ms_include_steps_pt,)


@app.cell
def _(get_state_exc, handle_exc, options_list):
    ms_exclude_steps_pt = mo.ui.multiselect( options=options_list, label="Exclude Steps", value=get_state_exc(), full_width=True, on_change=handle_exc) 
    return (ms_exclude_steps_pt,)


@app.cell
def visual__step_filters(list_available_steps):

    ms_include_steps_fc_a = mo.ui.multiselect.from_series(list_available_steps["step"], label='Include Steps -A-', full_width=True)
    ms_exclude_steps_fc_a = mo.ui.multiselect.from_series(list_available_steps["step"], label='Exclude Steps -A-', full_width=True)

    ms_include_steps_fc_b = mo.ui.multiselect.from_series(list_available_steps["step"], label='Include Steps -B-', full_width=True)
    ms_exclude_steps_fc_b = mo.ui.multiselect.from_series(list_available_steps["step"], label='Exclude Steps -B-', full_width=True)
    return (
        ms_exclude_steps_fc_a,
        ms_exclude_steps_fc_b,
        ms_include_steps_fc_a,
        ms_include_steps_fc_b,
    )


@app.cell
def sql__build_flowchart():
    #sql_build_flowchart = build_flowchart_structure(project={dropdown_projects.value}, exc=ms_exclude_steps_pt.value, inc=ms_include_steps_pt.value, meta_1=ms_meta_search_1.value)

    #if DEBUG:
    #    print(sql_build_flowchart)
    return


@app.cell
def _(dropdown_projects, end_date, sql_parts_pt, start_date):
    sql_build_flowchart = build_flowchart_structure(env=env, project=dropdown_projects.value, start_date=start_date.value, end_date=end_date.value, sql_parts=sql_parts_pt)
    return (sql_build_flowchart,)


@app.cell
def _(dropdown_projects, end_date, sql_parts_a, sql_parts_b, start_date):
    ##
    ## A/B Comaprisons
    ##

    sql_build_flowchart_fc_a = build_flowchart_structure(env=env, project=dropdown_projects.value, start_date=start_date.value, end_date=end_date.value, sql_parts=sql_parts_a)
    sql_build_flowchart_fc_b = build_flowchart_structure(env=env, project=dropdown_projects.value, start_date=start_date.value, end_date=end_date.value, sql_parts=sql_parts_b)
    return sql_build_flowchart_fc_a, sql_build_flowchart_fc_b


@app.cell
def _(sql_build_flowchart):
    dataframe_flowchart = mo.sql(
        f"""
        {sql_build_flowchart}
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (dataframe_flowchart,)


@app.cell
def _(sql_build_flowchart_fc_a):
    dataframe_flowchart_fc_a = mo.sql(
        f"""
        {sql_build_flowchart_fc_a}
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (dataframe_flowchart_fc_a,)


@app.cell
def _(sql_build_flowchart_fc_b):
    dataframe_flowchart_fc_b = mo.sql(
        f"""
        {sql_build_flowchart_fc_b}
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (dataframe_flowchart_fc_b,)


@app.function
def nodes_for_sql(data):

    nodes = set()

    for start, end, minutes in data:
        nodes.add(start)
        nodes.add(end)

    nodes_sql_list = str(nodes).replace('{', '(').replace('}', ')')

    return nodes_sql_list


@app.cell
def visual__create_flowchart(
    dataframe_flowchart,
    dropdown_projects,
    metric_selection_pt,
    switch_flowchart_orientation,
):


    def get_belongs_to(used_nodes, connection) -> str:

        _used_nodes = str(used_nodes).replace("{", "(").replace("}", ")") 

        belongs_to = connection.execute(
            f"""SELECT 
                    STEP, 
                    BELONGS_TO, 
                    SHAPE 
                FROM 
                    {env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.STEPS 
                WHERE 
                    SAFE_STEP IN {_used_nodes} AND PROJECT_ID = '{dropdown_projects.value}' 
                ORDER BY 
                    BELONGS_TO ASC 
            """).fetchall()

        # Group by BELONGS_TO


        groups = defaultdict(list)
        for step, group, shape in belongs_to:
            groups[group].append((step, shape))

        # Generate Mermaid subgraph structure
        _subgraphs_list = "\n"
        for group, steps in groups.items():
            if group != 'GLOBAL':
                _subgraphs_list += f"    subgraph {group}__________ \n"
                for step, shape in steps:
                    s = step.replace(' ', '_').replace("-", "_")
                    _subgraphs_list += f"        {s}[\"{step}_____\"]@{{ shape: {shape} }}\n"
                _subgraphs_list += "    end\n"

        return _subgraphs_list


    def generate_styles(used_nodes, connection) -> str:


        _used_nodes = str(used_nodes).replace("{", "(").replace("}", ")")

        with Exasol_Database_Engine.connect() as _con:
            _rows = _con.execute(f"SELECT * FROM {env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.STEPS WHERE SAFE_STEP IN {_used_nodes} AND PROJECT_ID = '{dropdown_projects.value}'").fetchall()
            _nodes = pl.DataFrame(_rows)


        styles = []

        for  row in _nodes.iter_rows(named=True):
            label = row["safe_step"]
            step = label.replace(" ", "_").replace("-", "_")
            bg = row["bg_color"] if pd.notnull(row["bg_color"]) else "None"
            fg = row["fg_color"] if pd.notnull(row["fg_color"]) else "None"
            styles.append(f"style {label} fill:{bg}, color:{fg}")



        node_styles = ""
        for style in styles:
            node_styles += style + "\n"

        return node_styles


    def create_journeys_flowchart(data, metric, connection) -> dict:

        mermaid_content = ''
    
        if switch_flowchart_orientation.value:
            mermaid_content = "flowchart LR\n"
        else:
            mermaid_content = "flowchart TD\n"
    
        metric = metric[0]

        # Track all nodes and their connections

        nodes = set()
        visited_nodes = set()
        edges = []


        for start, end, sum_transitions, avg_time, min_time, max_time, median_time, stddev_time in data.iter_rows():
            nodes.add(start)
            nodes.add(end)

            visited_nodes.add(start.replace(" ", "_").replace("-", "_").replace(".", "_"))
            visited_nodes.add(end.replace(" ", "_").replace("-", "_").replace(".", "_"))

            edges.append((start, end, sum_transitions, avg_time, min_time, max_time, median_time, stddev_time))

        if DEBUG:
            print(visited_nodes)
            print('')

        _subgraphs_list = get_belongs_to(visited_nodes, connection)

        mermaid_content += _subgraphs_list + "\n"
        print(mermaid_content)

        sankey_content = "\n \n sankey \n \n"

        # Add edges with weights
        i = 1
        for start, end, sum_transitions, avg_time, min_time, max_time, median_time, stddev_time in edges:
            safe_start = (
                start.replace(" ", "_").replace("-", "_").replace(".", "_")
            )
            safe_end = end.replace(" ", "_").replace("-", "_").replace(".", "_")

            # Format weight for display
            formatted_sum_transitions = f"{sum_transitions:,}"
            formatted_min_time = f"{float(min_time):.1f}"
            formatted_avg_time = f"{float(avg_time):.1f}"
            formatted_max_time = f"{float(max_time):.1f}"
            formatted_median_time = f"{float(median_time):.1f}"
            formatted_stddev_time = f"{float(stddev_time):.1f}"

            if metric == 'Number of Journeys':
                print_metric = formatted_sum_transitions
            elif metric == 'Average Transition Time':
                print_metric = formatted_avg_time
            elif metric == "Minimum Transition Time":
                print_metric = formatted_min_time
            elif metric == 'Maximum Transition Time':
                print_metric = formatted_max_time
            elif metric == 'Median Transition Time':
                print_metric = formatted_median_time
            elif metric == 'StdDev Transition Time':
                print_metric = formatted_stddev_time
            else:
                print_metric = formatted_sum_transitions

            mermaid_content += (
                f"""    {safe_start} e{i}@== ..........{print_metric}.......... ==>{safe_end} 
                        e{i}@{{ animate: true }}
            """
            )
            sankey_metric = 1 # int(print_metric.replace(',',''))
            sankey_content += f"{start}:,{end}:,{sankey_metric} \n"


            i = i + 1

        node_styles = generate_styles(visited_nodes, connection)

        flowchart = mermaid_content + "\n\n" + str(node_styles)

        return {
                 "flowchart": flowchart,
                 "sankey": sankey_content,

        }


    ##
    ##
    ##

    with Exasol_Database_Engine.connect() as _con:        

        mermaid_diagram = create_journeys_flowchart(dataframe_flowchart, metric_selection_pt.value, _con)['flowchart']
        mermaid_diagram += "\n\n"

        sankey_diagram = create_journeys_flowchart(dataframe_flowchart, metric_selection_pt.value, _con)['sankey']

    if DEBUG:
        print(mermaid_diagram)
        print(sankey_diagram)
    return (
        create_journeys_flowchart,
        generate_styles,
        get_belongs_to,
        mermaid_diagram,
        sankey_diagram,
    )


@app.cell
def _(sankey_diagram):
    x = mo.mermaid(sankey_diagram)
    return (x,)


@app.cell
def _(x):
    x
    return


@app.cell
def _(create_journeys_flowchart, dataframe_flowchart, metric_selection_pt):
    with Exasol_Database_Engine.connect() as _con:   
        print(create_journeys_flowchart(dataframe_flowchart, metric_selection_pt.value, _con)['flowchart'])
    return


@app.cell
def _(
    create_journeys_flowchart,
    dataframe_flowchart_fc_a,
    dataframe_flowchart_fc_b,
    metric_selection_a,
    metric_selection_b,
):
    with Exasol_Database_Engine.connect() as _con:        

        mermaid_diagram_fc_a = create_journeys_flowchart(dataframe_flowchart_fc_a, metric_selection_a.value, _con)['flowchart']
        mermaid_diagram_fc_a += "\n\n"

        mermaid_diagram_fc_b = create_journeys_flowchart(dataframe_flowchart_fc_b, metric_selection_b.value, _con)['flowchart']
        mermaid_diagram_fc_b += "\n\n"
    return mermaid_diagram_fc_a, mermaid_diagram_fc_b


@app.cell
def _(dropdown_projects, individual_journey_input_id):
    ## Individual Journey

    sql_individual_journey = f"""
    WITH process_chains AS (
        SELECT
            EVENT_ID,
            EVENT_TIME AS FROM_TIME,
            J.STEP AS FROM_STEP,
            LEAD(J.STEP) OVER (PARTITION BY EVENT_ID ORDER BY EVENT_TIME) AS TO_STEP,
            LEAD(EVENT_TIME) OVER (PARTITION BY EVENT_ID ORDER BY EVENT_TIME) AS TO_TIME,
            S.SHAPE AS SHAPE
        FROM
            {env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']} J JOIN STEPS S ON (J.STEP = S.STEP)
        WHERE 
            --PROJECT_ID = '{dropdown_projects.value}' AND
            EVENT_ID = '{individual_journey_input_id.value}'
        ORDER BY 
            EVENT_ID, FROM_TIME
    )

    -- Final result

    SELECT 
            EVENT_ID,
            FROM_TIME,
            FROM_STEP,
            TO_STEP,
            TO_TIME,
            MINUTES_BETWEEN(TO_TIME, FROM_TIME) as DUR_MINS,
            SHAPE
    FROM process_chains
    WHERE TO_STEP IS NOT NULL

    """

    sql_individual_score =f"""
    SELECT
        SUM(SCORE)                                       AS SCORE_OF_JOURNEY,
        COUNT(EVENT_ID)                                  AS NUM_EVENTS,
        SECONDS_BETWEEN(MAX(EVENT_TIME),MIN(EVENT_TIME)) AS DURATION_SECONDS,
        COUNT(DISTINCT J.STEP)                           AS NUM_NODES
    FROM  
        STEPS S JOIN {env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']} J ON S.STEP_ID = J.STEP_ID
    WHERE
        J.PROJECT_ID = '{dropdown_projects.value}' AND
        EVENT_ID = '{individual_journey_input_id.value}'


    """

    sql_get_meta = f"""
        SELECT
            META_1,
            EVENT_TIME
        FROM
            {env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}
        WHERE
            PROJECT_ID = '{dropdown_projects.value}' AND
            EVENT_ID = '{individual_journey_input_id.value}'
        LIMIT 1

    """
    return


@app.cell
def sql_build_flowchart_individual(
    dropdown_projects,
    individual_journey_input_id,
):
    dataframe_flowchart_individual = mo.sql(
        f"""
        WITH
            process_chains AS (
                SELECT
                    CAST(EVENT_ID AS VARCHAR(32)) AS EVID,
                    EVENT_TIME AS FROM_TIME,
                    J.STEP AS FROM_STEP,
                    LEAD (J.STEP) OVER (
                        PARTITION BY
                            EVENT_ID
                        ORDER BY
                            EVENT_TIME
                    ) AS TO_STEP,
                    LEAD (EVENT_TIME) OVER (
                        PARTITION BY
                            EVENT_ID
                        ORDER BY
                            EVENT_TIME
                    ) AS TO_TIME,
                    S.SHAPE
                FROM
                    {env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.JOURNEYS J
                    JOIN {env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.STEPS S ON (J.STEP = S.STEP)
                WHERE
                    S.PROJECT_ID = '{dropdown_projects.value}'
                    AND local.EVID = '{individual_journey_input_id.value}'
            )
            -- Final result
        SELECT
            EVID,
            FROM_TIME,
            FROM_STEP,
            TO_STEP,
            TO_TIME,
            MINUTES_BETWEEN (TO_TIME, FROM_TIME) as DUR_MINS,
            SHAPE
        FROM
            process_chains
        WHERE
            TO_STEP IS NOT NULL
        ORDER BY
            FROM_TIME ASC
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (dataframe_flowchart_individual,)


@app.cell
def sql__journey_metadata(dropdown_projects, individual_journey_input_id):
    dataframe_journey_metadata = mo.sql(
        f"""
        SELECT
            META_1,
            EVENT_TIME
        FROM
            {env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.JOURNEYS
        WHERE
            PROJECT_ID = '{dropdown_projects.value}'
            AND EVENT_ID = '{individual_journey_input_id.value}'
        LIMIT
            1
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (dataframe_journey_metadata,)


@app.cell
def _(
    dataframe_flowchart_individual,
    generate_styles,
    get_belongs_to,
    switch_flowchart_orientation,
):
    def create_individual_journey_flowchart(data, connection) -> str:

        if data.is_empty():
            pass
        else:

            mermaid_content_individual_journey = ''

            if switch_flowchart_orientation.value:
                _orientation = "flowchart LR\n"
            else:
                _orientation = "flowchart TD\n"

            # To avoid cycles, store visited edges

            #mermaid_content = "flowchart LR"
            visited_nodes = set()
            transition_num = 1

            # iterate over Polars rows as Python tuples
            x = 1
            for i, row in enumerate(data.iter_rows(named=True), start=1):
                # row is a dict-like object with column names
                evid        = row["evid"]
                start_time  = row["from_time"]
                src         = row["from_step"]
                dst         = row["to_step"]
                end_time    = row["to_time"]
                duration    = row["dur_mins"]
                shape       = row["shape"]

                # Skip if either node was already used
                if src in visited_nodes or dst in visited_nodes:
                    continue

                # Normalize node names
                src_clean = src.replace(" ", "_").replace("-", "_")
                dst_clean = dst.replace(" ", "_").replace("-", "_")

                visited_nodes.update([src_clean, dst_clean])

                # Convert times
                st = datetime.fromisoformat(start_time).time()
                et = datetime.fromisoformat(end_time).time()

                # Build Mermaid-like content block
                mermaid_content_individual_journey += (
                    f"\n    {src_clean} e{transition_num}@== ...Step-{transition_num} : {duration} mins....... ==>"
                    f"{dst_clean}\n"
                    f"e{transition_num}@{{animate: true}}"

                    )

                transition_num += 1
                x = x + 1

            _subgraphs_list = get_belongs_to(visited_nodes, connection)
            node_styles = generate_styles(visited_nodes, connection)

            mermaid_content_individual_journey = _orientation + "\n" + _subgraphs_list + "\n\n" + mermaid_content_individual_journey + "\n\n" +  node_styles

            return mermaid_content_individual_journey



    individual_journey_flowchart = create_individual_journey_flowchart(dataframe_flowchart_individual, Exasol_Database_Engine)

    if DEBUG:
        print(individual_journey_flowchart)
    return (individual_journey_flowchart,)


@app.cell
def sql__statistics_journey(dropdown_projects, individual_journey_input_id):
    statistics_journey = mo.sql(
        f"""
        SELECT
            TO_CHAR(EVENT_TIME, 'YYYY-MM-DD')                AS EVENT_DATE,	
            TO_CHAR(MIN(EVENT_TIME), 'HH:MI:SS')             AS START_TIME,
            TO_CHAR(MAX(EVENT_TIME), 'HH:MI:SS')             AS END_TIME,
            SUM(SCORE)                                       AS SCORE_OF_JOURNEY,
            COUNT(EVENT_ID)                                  AS NUM_EVENTS,
            SECONDS_BETWEEN(MAX(EVENT_TIME),MIN(EVENT_TIME)) AS DURATION_SECONDS,
            COUNT(DISTINCT J.STEP)                           AS NUM_NODES
        FROM  
            {env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.STEPS S JOIN {env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.JOURNEYS J ON S.STEP_ID = J.STEP_ID
        WHERE
            J.PROJECT_ID = '{dropdown_projects.value}' AND
            EVENT_ID = '{individual_journey_input_id.value}'
        GROUP BY
        	local.EVENT_DATE
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (statistics_journey,)


@app.cell
def _(
    dataframe_journey_metadata,
    individual_journey_input_id,
    meta_descriptions,
    statistics_journey,
):
    if individual_journey_input_id.value != '':
        _process_date = statistics_journey['event_date'][0]

        _process_date = mo.stat(
            label="Date of Process",
            bordered=True,
            caption=f"YYYY-MM-DD",
            value=statistics_journey['event_date'][0]
        )

        _start_time = mo.stat(
            label="Start of Process",
            bordered=True,
            caption=f"-",
            value=statistics_journey['start_time'][0]
        )

        _end_time = mo.stat(
            label="End of Process",
            bordered=True,
            caption=f"-",
            value=statistics_journey['end_time'][0]
        )

        _individual_score = mo.stat(
            label="Individual Score",
            bordered=True,
            caption=f"Sum of all step scores",
            value=statistics_journey['score_of_journey'][0]
        )

        _num_steps = mo.stat(
            label="Number of Steps",
            bordered=True,
            caption=f"Number of Steps",
            value=statistics_journey['num_events'][0]
        )

        _journey_duration = mo.stat(
            label="Journey Duration",
            bordered=True,
            caption=f"HH:MM:SS",
            value=str(timedelta(seconds=int(statistics_journey['duration_seconds'][0])))    
        )
        _num_nodes = mo.stat(
            label="Number of Nodes",
            bordered=True,
            caption=f"Nodes",
            value=statistics_journey['num_nodes'][0]
        )
        _meta_1 = mo.stat(
            label=meta_descriptions['meta_1'][0],
            bordered=True,
            caption=f"-",
            value=dataframe_journey_metadata['meta_1'][0]
        )

        individual_journey_statistics = mo.vstack([
            mo.hstack([_process_date, _start_time, _end_time, _journey_duration, _individual_score, _num_steps, _num_nodes, _meta_1], gap=1, align="start", justify="start", widths="equal"),
            #mo.hstack([_individual_score, _num_steps, _num_nodes, _meta_1], gap=1, align="start", justify="start", widths="equal"),      
        ])
    return (individual_journey_statistics,)


@app.cell
def _():
    ################
    ## AI Section ##
    ################
    return


@app.cell
def _(dropdown_projects):
    df_system_prompt = mo.sql(
        f"""
        SELECT
        	PROMPT as prompt
        FROM
            {env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.PROMPTS
        WHERE
        	PROJECT_ID = '{dropdown_projects.value}'
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (df_system_prompt,)


@app.cell
def llm__single_flowchart(
    ai_button,
    df_system_prompt,
    mermaid_diagram,
    slider_temperature_llm,
):

    from tools.llm.llm_flowchart import llm_flowchart_analysis

    llm_result_single_flowchart = ''

    _server = env['KEA_PROCESS_INSIGHTS_LLM_SERVER_URL']
    _token = env['KEA_PROCESS_INSIGHTS_LLM_API_TOKEN']
    _prompt = df_system_prompt['prompt'][0]

    if ai_button.value:
        #print("Inferencing LLM")
        llm_result_single_flowchart=ResultText(llm_flowchart_analysis(_server, _token, _prompt, mermaid_diagram, slider_temperature_llm))
    return (llm_result_single_flowchart,)


@app.cell
def _():
    ######################
    ## Settings Section ##
    ######################
    return


@app.cell
def _(df_system_prompt):
    text_arera_system_prompt = mo.ui.text_area(value=df_system_prompt['prompt'][0], 
                                               placeholder = 'System Prompt goees here...', 
                                               label = 'System Prompt', 
                                               full_width = True, 
                                               rows = 24, 
                                               max_length = 2048, 
                                               debounce = 1)




    button_save_system_prompt = mo.ui.run_button(label='Save System Prompt for AI Overview', kind='success', on_change=save_system_prompt)
    return button_save_system_prompt, text_arera_system_prompt


@app.cell
def _(button_save_system_prompt, text_arera_system_prompt):
    settings = mo.vstack([mo.accordion(
        {
            "### Definition - AI System Prompt": mo.vstack([text_arera_system_prompt, button_save_system_prompt]),
            "### Configuration - Steps": mo.md("Nothing!"),
            "### Configuration - Metas": mo.vstack([mo.md('Metas')]),
        },

    )],align='stretch')
    return (settings,)


@app.cell
def _(dropdown_projects, end_date, start_date):
    tst1 = mo.sql(
        f"""
        SELECT
            TO_CHAR (CAST(EVENT_TIME AS DATE), 'YYYY-MM-DD') AS DATE_DAY,
            COUNT(DISTINCT EVENT_ID) AS CNT
        FROM
            {env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.JOURNEYS
        WHERE
            PROJECT_ID = '{dropdown_projects.value}'
            AND EVENT_TIME BETWEEN DATE '{start_date.value}' AND DATE  '{end_date.value}'
        GROUP BY
            local.DATE_DAY
        ORDER BY
            DATE_DAY ASC
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (tst1,)


@app.cell
def _(tst1):
    # replace _df with your data source
    chart_1 = (
        alt.Chart(tst1)
        .mark_line()
        .encode(
            x=alt.X(field='date_day', type='temporal', sort='ascending', timeUnit='monthdate'),
            y=alt.Y(field='cnt', type='quantitative', sort='ascending'),
            tooltip=[
                alt.Tooltip(field='date_day', timeUnit='monthdate', title='date_day'),
                alt.Tooltip(field='cnt', format=',.0f')
            ]
        )
        .properties(
            height=512,
            width='container',
            config={
                'axis': {
                    'grid': False
                }
            }
        )
    )
    return (chart_1,)


@app.cell
def _(dropdown_projects, end_date, start_date):
    statistics_graph_steps = mo.sql(
        f"""
        SELECT
            STEP,
            COUNT( STEP) AS CNT
        FROM
            {env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.JOURNEYS
        WHERE
            PROJECT_ID = '{dropdown_projects.value}'
            AND EVENT_TIME BETWEEN DATE '{start_date.value}' AND DATE  '{end_date.value}'
        GROUP BY
            STEP
        ORDER BY
            CNT DESC
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (statistics_graph_steps,)


@app.cell
def _(statistics_graph_steps):
    # replace _df with your data source

    chart_2 = (
        alt.Chart(statistics_graph_steps)
        .mark_bar()
        .encode(
            x=alt.X(field='cnt', type='quantitative', aggregate='mean'),
            y=alt.Y(field='step', type='ordinal', stack=False, sort='ascending'),
            color=alt.Color(field='cnt', type='quantitative', scale={
                'scheme': 'redblue'
            }),
            tooltip=[
                alt.Tooltip(field='step'),
                alt.Tooltip(field='cnt', aggregate='mean', format=',.0f'),
                alt.Tooltip(field='cnt', format=',.0f')
            ]
        )
        .properties(
            height=512,
            width='container',
            config={
                'axis': {
                    'grid': False
                }
            }
        )
    )
    return (chart_2,)


@app.cell
def _(chart_1, chart_2, chart_3, path_statistics):
    statistics_row_1 = mo.hstack(
        [chart_1, chart_2],
        widths="equal",
        gap=2,
    )

    statistics_row_2 = mo.hstack([chart_3, path_statistics], widths='equal', gap=2, align='stretch')
    return statistics_row_1, statistics_row_2


@app.cell
def _(dropdown_projects, end_date, start_date):
    statistics_heatmap_steps_month = mo.sql(
        f"""
        SELECT
            TO_CHAR (CAST(EVENT_TIME AS DATE), 'YYYY-MM') AS DATE_MONTH,
            STEP,
            COUNT( STEP) AS CNT
        FROM
            {env['KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA']}.JOURNEYS
        WHERE
            PROJECT_ID = '{dropdown_projects.value}'
            AND EVENT_TIME BETWEEN DATE '{start_date.value}' AND DATE  '{end_date.value}'
        GROUP BY
            local.DATE_MONTH, STEP
        ORDER BY
            CNT DESC
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (statistics_heatmap_steps_month,)


@app.cell
def _(statistics_heatmap_steps_month):
    # replace _df with your data source
    chart_3 = (
        alt.Chart(statistics_heatmap_steps_month)
        .mark_rect()
        .encode(
            x=alt.X(field='date_month', type='nominal'),
            y=alt.Y(field='step', type='nominal'),
            color=alt.Color(field='cnt', type='quantitative', scale={
                'scheme': 'set1'
            }, aggregate='sum'),
            row=alt.Row(field='step', type='nominal', bin={
                'maxbins': 6
            }),
            column=alt.Column(field='date_month', type='nominal', bin={
                'maxbins': 6
            }),
            tooltip=[
                alt.Tooltip(field='date_month'),
                alt.Tooltip(field='step'),
                alt.Tooltip(field='cnt', aggregate='sum', format=',.0f')
            ]
        )
        .resolve_scale(x='independent', y='independent')
        .properties(
            height=512,
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    )
    return (chart_3,)


@app.cell
def _():
    return


@app.cell
def _():
    ##
    ## Form for new Steps
    ##


    step_markdown = mo.md(
        '''
        - Description: {description}
        - Group: {belongs_to}
        - Background Color:  {bgcolor}
        - Text Color:  {color}
        - Shape:  {shape}
        '''
    )
    step_form = mo.ui.batch(
        step_markdown, { 
                    "description": mo.ui.text(value="DIRK"),
                    "belongs_to": mo.ui.text(value='GOV'),     
                    "bgcolor": mo.ui.dropdown(options=["black", "blue", "cyan", "green", "orange"], value='blue'), 
                    "color": mo.ui.dropdown(options=["black", "white"], value='white'), 
                    "shape": mo.ui.dropdown(options=["hex", "odd", "rounded", "stadium"], value='rounded')
        }
    )
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _():
    statistics_path_analysis = mo.sql(
        f"""
        WITH ordered_paths AS (
            SELECT
                EVENT_ID,
                LISTAGG(STEP, ' -> ')
                    WITHIN GROUP (ORDER BY EVENT_TIME ASC) AS full_path,
                COUNT(*) AS path_length
            FROM EXASOL_DIB_PROCESS_MINING.JOURNEYS
            GROUP BY EVENT_ID
        ),

        distinct_paths AS (
            SELECT
                full_path,
                path_length,
                COUNT(*) AS journey_count
            FROM ordered_paths
            GROUP BY full_path, path_length
        )

        SELECT
            journey_count,
            path_length,
            full_path
        FROM distinct_paths
        WHERE journey_count >= 1000
        ORDER BY  journey_count DESC
        LIMIT 10000;
        """,
        output=False,
        engine=Exasol_Database_Engine
    )
    return (statistics_path_analysis,)


@app.cell
def _():
    path_statistics_bin_dropdown = dropdown = mo.ui.dropdown(
        options=[1, 2, 3, 4, 5, 10, 15], value=2, label="Choose Bin-Size"
    )
    return (path_statistics_bin_dropdown,)


@app.cell
def _(path_statistics_bin_dropdown, statistics_path_analysis):
    # replace _df with your data source
    path_statistics_chart = (
        alt.Chart(statistics_path_analysis)
        .mark_bar()
        .encode(
            x=alt.X(field='journey_count', type='quantitative'),
            y=alt.Y(field='path_length', type='quantitative', bin={
                'step': path_statistics_bin_dropdown.value
            }),
            color=alt.Color(field='journey_count', type='quantitative', scale={
                'scheme': 'bluegreen'
            },aggregate='sum'),
            tooltip=[
                alt.Tooltip(field='journey_count', format=',.0f'),
                alt.Tooltip(field='path_length', format=',.0f', bin={
                    'step': path_statistics_bin_dropdown.value
                }),
                alt.Tooltip(field='journey_count', aggregate='sum', format=',.0f')
            ]
        )
        .properties(
            height=512,
            width='container',
            config={
                'axis': {
                    'grid': False
                }
            }
        )
    )
    return (path_statistics_chart,)


@app.cell
def _(path_statistics_bin_dropdown, path_statistics_chart):
    path_statistics = mo.hstack([mo.vstack([path_statistics_bin_dropdown, path_statistics_chart])],widths=[1,1])
    return (path_statistics,)


@app.cell
def _():
    mo.mermaid('''

    sankey

    ENTER Baggage Drop:,LEAVE Baggage Drop:,34995
    ENTER Boarding Gate Dom:,BOARD Aircraft Dom:,86506 
    ENTER Boarding Gate Dom:,DENIED Boarding Dom:,4650
    ENTER Boarding Gate Int:,BOARD Aircraft Int:,28557
    ENTER Boarding Gate Int:,DENIED Boarding Int:,1506
    ENTER Check-In:,LEAVE Check-In:,36793
    ENTER Departure Hall:,ENTER Security Check:,49430
    ENTER Departure Hall:,ENTER Baggage Drop:,34995 
    ENTER Departure Hall:,LEAVE Departure Hall:,2413 
    ENTER Departure Hall:,ENTER Check-In:,36793
    ENTER Dining Area Dom:,LEAVE Dining Area Dom:,43268 
    ENTER Dining Area Int:,LEAVE Dining Area Int:,11338 
    ENTER Duty Free Dom:,LEAVE Duty Free Dom:,40918 
    ENTER Duty Free Int:,LEAVE Duty Free Int:,13646 
    ENTER Lounge Dom:,LEAVE Lounge Dom:,31595 
    ENTER Lounge Int:,LEAVE Lounge Int:,11163 
    ENTER Passport Control:,LEAVE Passport Control:,30065 
    ENTER Security Check:,LEAVE Security Check:,121218
    LEAVE Baggage Drop:,ENTER Security Check:,34995 
    LEAVE Check-In:,ENTER Security Check:,36793 
    LEAVE Dining Area Dom:,ENTER Boarding Gate Dom:,32338 
    LEAVE Dining Area Dom:,ENTER Duty Free Dom:,9117 
    LEAVE Dining Area Dom:,ENTER Lounge Dom:,7065 
    LEAVE Dining Area Int:,ENTER Lounge Int:,1741 
    LEAVE Dining Area Int:,ENTER Boarding Gate Int:,8484 
    LEAVE Dining Area Int:,ENTER Duty Free Int:,2411 
    LEAVE Duty Free Dom:,ENTER Dining Area Dom:,15054 
    LEAVE Duty Free Dom:,ENTER Boarding Gate Dom:,21271 
    LEAVE Duty Free Dom:,ENTER Lounge Dom:,9007 
    LEAVE Duty Free Int:,ENTER Lounge Int:,2991 
    LEAVE Duty Free Int:,ENTER Boarding Gate Int:,7156 
    LEAVE Duty Free Int:,ENTER Dining Area Int:,4977 
    ''')
    return


@app.cell
def _(sankey_diagram):
    mo.mermaid(f"{sankey_diagram}")
    return


@app.cell
def _(sankey_diagram):
    print(sankey_diagram)
    return


if __name__ == "__main__":
    app.run()
