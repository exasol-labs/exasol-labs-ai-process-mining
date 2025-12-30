########################
## Statistics Widgets ##
########################


import marimo as mo


def total_statistics_widgets(df_total, df_steps):

  earliest_date = mo.stat(
      label="Earliest Event Date",
      bordered=True,
      caption=f"Recorded Day",
      value=df_total['earliest_eventdate'][0],
  )
  
  most_recent_date = mo.stat(
      label="Most Recent Event Date",
      bordered=True,
      caption=f"Recorded Day",
      value=df_total['most_recent_eventdate'][0],
  )
  
  unique_journeys_raw = mo.stat(
      label="Unique Processes",
      bordered=True,
      caption=f"Individual identifiers",
      value=df_total['num_journeys'][0]
  )
  
  total_steps_raw = mo.stat(
      label="Total Number of Steps",
      bordered=True,
      caption=f"Steps",
      value=df_steps['total_steps'][0]
  )
  
  distinct_steps = mo.stat(
      label="Distinct Steps",
      bordered=True,
      caption=f"Steps",
      value=df_steps['distinct_steps'][0]
  )
  
  min_journey_time_raw = mo.stat(
      label="Minimum Process Time",
      bordered=True,
      caption=f"minutes",
      value=df_total['min_duration_minutes'][0]
  )
  
  median_journey_time_raw = mo.stat(
      label="Median of Process Time",
      bordered=True,
      caption=f"minutes",
      value=df_total['median_duration_minutes'][0]
  )
  
  average_journey_time_raw = mo.stat(
      label="Average Process Time",
      bordered=True,
      caption=f"minutes",
      value=df_total['avg_duration_minutes'][0]
  )
  
  maximum_journey_time_raw = mo.stat(
      label="Maximum Process Time",
      bordered=True,
      caption=f"minutes",
      value=df_total['max_duration_minutes'][0]
  )
  
  variance_journey_time_raw = mo.stat(
      label="Variance Process Time",
      bordered=True,
      caption=f"minutes",
      value=df_total['stddev_duration_minutes'][0]
  )
  
  total_statistics_1 = mo.hstack(
      [earliest_date, most_recent_date, unique_journeys_raw, total_steps_raw, distinct_steps],
      widths="equal",
      gap=1,
  )
  
  total_statistics_2 = mo.hstack(
      [min_journey_time_raw, median_journey_time_raw, average_journey_time_raw, maximum_journey_time_raw, variance_journey_time_raw],
      widths="equal",
      gap=1,
  )  

  return {
          "total_statistics_1": total_statistics_1,
          "total_statistics_2": total_statistics_2,
         }


#########################
## Filtered Statistics ##
#########################

def filtered_statistics_widgets(df_1, df_2, start_date, end_date) -> dict:

  earliest_date = mo.stat(
      label="Earliest Event Date",
      bordered=True,
      caption=f"Recorded Day",
      value=start_date,
  )
  
  most_recent_date = mo.stat(
      label="Most Recent Event Date",
      bordered=True,
      caption=f"Recorded Day",
      value=end_date,
  )

  
  _unique_journeys_flt = mo.stat(
      label="Unique Processes",
      bordered=True,
      caption=f"Individual identifiers",
      value=df_1['num_journeys'][0],
  )

  _total_steps_flt = mo.stat(
      label="Total Number of Steps",
      bordered=True,
      caption=f"Number of Steps",
      value=df_2['num_steps'][0],
  )

  _distinct_steps_flt = mo.stat(
      label="Distinct Steps",
      bordered=True,
      caption=f"Steps",
      value=df_2['num_distinct_steps'][0]
  )

  _min_journey_time_flt = mo.stat(
      label="Minimum Processes Time",
      bordered=True,
      caption=f"minutes",
      value=df_1['min_duration_minutes'][0],
  )
  
  _average_journey_time_flt = mo.stat(
      label="Average Processes Time",
      bordered=True,
      caption=f"minutes",
      value=df_1['avg_duration_minutes'][0],
  )

  _median_journey_time_flt = mo.stat(
      label="Median of Processes Time",
      bordered=True,
      caption=f"minutes",
      value=df_1['median_duration_minutes'][0],
  )
  
  _maximum_journey_time_flt = mo.stat(
      label="Maximum Processes Time",
      bordered=True,
      caption=f"minutes",
      value=df_1['max_duration_minutes'][0],
  )
  
  _variance_journey_time_flt = mo.stat(
      label="Variance of Processes Time",
      bordered=True,
      caption=f"minutes",
      value=df_1['stddev_duration_minutes'][0],
  )


  
  
  filtered_statistics_1 = mo.hstack(
      [earliest_date, most_recent_date, _unique_journeys_flt, _total_steps_flt, _distinct_steps_flt],
      widths="equal",
      gap=1,
  )

  filtered_statistics_2 = mo.hstack(
      [ _min_journey_time_flt, _median_journey_time_flt, _average_journey_time_flt, _maximum_journey_time_flt, _variance_journey_time_flt],
      widths="equal",
      gap=1,
  )

  return {
          "filtered_statistics_1": filtered_statistics_1,
          "filtered_statistics_2": filtered_statistics_2,
         }









  