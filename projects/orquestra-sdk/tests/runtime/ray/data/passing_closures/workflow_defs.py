################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import orquestra.sdk as sdk


@sdk.task
def make_array(values):
    # We import things here because module-level imports fail at the moment.
    import numpy as np

    return np.array(values)


@sdk.task
def make_cost_fn(target_array):
    # We import things here because module-level imports fail at the moment.
    import numpy as np

    def _cost_fn(params_array):
        # Imagine this takes the params array, evaluates the optimization
        # problem at hand, and returns the cost value.
        #
        # Notice it uses a closured variable.
        diffs = params_array - target_array
        return np.sum(diffs)

    return _cost_fn


@sdk.task
def eval_cost_fn(cost_fn, params):
    return cost_fn(params)


@sdk.workflow
def passing_closures_wf():
    target_array = make_array([1.0, 3.0, 3.0, 7.0])
    cost_fn = make_cost_fn(target_array)
    cost1 = eval_cost_fn(cost_fn, make_array([1, 4, 4, 7]))
    cost2 = eval_cost_fn(cost_fn, make_array([1, 3, 4, 7]))
    cost3 = eval_cost_fn(cost_fn, make_array([1, 3, 3, 7]))

    return [cost1, cost2, cost3, cost_fn]
