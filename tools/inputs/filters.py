import marimo as mo

##
## Disallow the selection of identical steps in exclude and include filter
## --
## Filter Handling for Single Flowchart
##



get_state_inc, set_state_inc = mo.state([])
get_state_exc, set_state_exc = mo.state([])

def handle_inc(new_value):
    """
    Handler for the include multiselect (Single Process View).
    Removes any newly selected items from the second multiselect's value.
    """

    current_value_exc = get_state_exc()
    
    # Find which items in new_value are not in current_value2
    
    to_remove_from_exc = [item for item in new_value if item in current_value_exc]
    
    if to_remove_from_exc:
      
        updated_value_exc = [item for item in current_value_exc if item not in to_remove_from_exc]
        set_state_exc(updated_value_exc)
      
    set_state_inc(new_value)


def handle_exc(new_value):
    """
    Handler for the second multiselect.
    Removes any newly selected items from the first multiselect's value.
    """
  
    current_value_inc = get_state_inc()
  
    # Find which items in new_value are not in current_value1
    to_remove_from_inc = [item for item in new_value if item in current_value_inc]
  
    if to_remove_from_inc:
        updated_value_inc = [item for item in current_value_inc if item not in to_remove_from_inc]
        set_state_inc(updated_value_inc)
      
    set_state_exc(new_value)