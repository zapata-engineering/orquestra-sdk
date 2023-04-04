Compute Engine Resource Management
=======================

Workflows submitted to run on Compute Engine can specify the computational resources they require, enabling precise management of resources and costs.

.. note::
    Resource management is supported only when executing on Compute Engine runtimes. Tasks and workflows defined with these parameters may still be executed on other runtimes, however the resource specification will be ignored.

Setting Task Resources
----------------------

Required hardware resources are configured on a per-task basis by setting the ``resources`` field of the task decorator:

.. code-block::
   :caption: task resource request example

    @sdk.task(
        resources=sdk.Resources(cpu="100m", memory="1Gi", disk="10Gi", gpu="1")
    )
    def my_task():
        ...

``resources`` expects a ``sdk.Resources()`` object that specifies some or all of:
* number of cores (``cpu``)
* amount of RAM (``memory``)
* disk space (``disk``)
* number of GPU computing units (``gpu``)

The example above specifies a task that requires 100 milicores (or 0.1 cores), 1 Gb of RAM, 10 Gb of disk space, and access to a GPU.

Setting Workflow Resources
--------------------------

Resources can also be configured at the workflow definition level using the same syntax as with tasks:

.. code-block::
    :caption: workflow resource request example

    @sdk.workflow(
        resources=sdk.Resources(cpu="100m", memory="1Gi", disk="10Gi", gpu="1")
    )
    def my_workflow():
        ...

In most cases, defining resources in this way will be unnecessary as Compute Engine can infer the overall resource requirements from the aggregated requirements of individual tasks. The primary use-case for this facility is to provision additional resources that aren't covered by the task definitions, such as when tasks spawn additional processes.


Troubleshooting Common Resource Issues
--------------------------------------

Due to the way Ray's RLLib works, a deadlock can be created on Compute Engine if a task attempts to spawn additional processes, notably via the DNQ ``rollouts`` facility. Resources requested in a task definition are bound to the task process, so additional processes can rapidly exhaust the provisioned resources. In these cases, additional resources should be specified in the workflow decorator.

.. code-block::
    :caption: Example: override workflow resources.
    @sdk.task(resources=...)                    # task resources requested.
    def task():
        config = DQNConfig()
        ...
        config.rollouts(num_rollout_workers=2)  # additional processes do not have
        ...                                     # access to task resources.
        return results

    @sdk.workflow(resources=...)                # Override the aggregated task
    def wf():                                   # resources to provision additional
        results = []                            # resources for the additional
        for _ in range(5):                      # processes.
            results.append(task())
