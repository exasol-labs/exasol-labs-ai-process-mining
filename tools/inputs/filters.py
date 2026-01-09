##############################################################################
## Disallow the selection of identical steps in exclude and include filters ##
##############################################################################

import marimo as mo

###################################
## Flowchart -A- Filter Handling ##
###################################

get_state_fc_a, set_state_fc_a = mo.state({
    "include": [],
    "exclude": [],
})

def handle_inc_fc_a(new_value: list):
    state = get_state_fc_a()

    new_inc = new_value
    new_exc = [x for x in state["exclude"] if x not in new_inc]

    set_state_fc_a({
        "include": new_inc,
        "exclude": new_exc,
    })


def handle_exc_fc_a(new_value: list):
    state = get_state_fc_a()

    new_exc = new_value
    new_inc = [x for x in state["include"] if x not in new_exc]

    set_state_fc_a({
        "include": new_inc,
        "exclude": new_exc,
    })


###################################
## Flowchart -B- Filter Handling ##
###################################

get_state_fc_b, set_state_fc_b = mo.state({
    "include": [],
    "exclude": [],
})

def handle_inc_fc_b(new_value: list):
    state = get_state_fc_b()

    new_inc = new_value
    new_exc = [x for x in state["exclude"] if x not in new_inc]

    set_state_fc_b({
        "include": new_inc,
        "exclude": new_exc,
    })


def handle_exc_fc_b(new_value: list):
    state = get_state_fc_b()

    new_exc = new_value
    new_inc = [x for x in state["include"] if x not in new_exc]

    set_state_fc_b({
        "include": new_inc,
        "exclude": new_exc,
    })


  