Resource Management
=======================

Workflows submitted to run on Compute Engine and Local Ray can specify the computational resources they require, enabling precise management of resources and costs.

.. note::
    Resource management is supported only when executing on Compute Engine and Local Ray runtimes. Tasks and workflows defined with these parameters may still be executed on other runtimes, however the resource specification will be ignored.

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
* ``cpu``: number of cores.
* ``memory``: amount of RAM (bytes).
* ``disk``: disk space (bytes).
* ``gpu``: whether access to a gpu unit is required (``1`` if a GPU is required, ``0`` otherwise).

Amounts of cpu and memory resources are specified by a string comprising a floating point value, and, optionally, a modifier to the base unit ('byte' in the case of ``memory`` and ``disk`` requests, 'cores' in the case of ``cpu`` requests). The modifier can be a SI (metric), or IEC (binary) multiplier as detailed in the table below. So ``disk="10k"`` will be interpreted as '10 kilobytes', while ``cpu="10k"`` would request 10^7 cores.

.. table:: unit multipliers
    :widths: auto

    +---------+-------+--------+-------+
    |         | Name  | String | Value |
    +=========+=======+========+=======+
    | Binary  | kibi  | Ki     | 2^10  |
    |         | mibi  | Mi     | 2^20  |
    |         | gibi  | Gi     | 2^30  |
    |         | tebi  | Ti     | 2^40  |
    |         | pebi  | Pi     | 2^50  |
    |         | exbi  | Ei     | 2^60  |
    +---------+-------+--------+-------+
    | Metric  | nano  | n      | 10^-9 |
    |         | micro | u      | 10^-6 |
    |         | milli | m      | 10^-3 |
    |         | kilo  | k      | 10^3  |
    |         | mega  | M      | 10^6  |
    |         | giga  | G      | 10^9  |
    |         | tera  | T      | 10^12 |
    |         | peta  | P      | 10^15 |
    |         | exa   | E      | 10^18 |
    +---------+-------+--------+-------+

Convention is to use binary prefixes for memory resource requests (``disk`` and ``memory``), and decimal prefixes to specify the number of cores. The task resource request example above specifies a task that requires 100 milicores (or 0.1 cores), 1 gibibyte of RAM (2^30 bytes), 10 gibibytes of disk space(1.25*2^33 bytes), and access to a GPU.

.. note:: mixing unit prefixes
    Binary and decimal units can be used interchangeably, however this can occasionally cause confusion, and care must be taken when specifying these parameters. For example, a memory request of ``100m`` specifies not 100 megabytes, but 100 millibytes, or 0.1 bytes.

Setting Workflow Resources
--------------------------

Resources can also be configured at the workflow definition level using the same syntax as with tasks, with one difference - the ``sdk.Resources()`` object my additionally specify a number of nodes to be requested for the workflow. The full parameter list is therefore:
* ``cpu``: number of cores.
* ``memory``: amount of RAM (bytes).
* ``disk``: disk space (bytes).
* ``gpu``: whether access to a gpu unit is required (``1`` if a GPU is required, ``0`` otherwise).
* ``nodes``: the number of nodes requested.

.. code-block::
    :caption: workflow resource request example

    @sdk.workflow(
        resources=sdk.Resources(cpu="100m", memory="1Gi", disk="10Gi", gpu="1", nodes=5)
    )
    def my_workflow():
        ...


.. note:: nodes
    Note that unlike the other parameters, ``nodes`` must be an integer rather than a string.

Currently, the workflow resource request is only utilised by Compute Engine.
If resources are not provided, Compute Engine will infer the overall resource requirements from the aggregated requirements of individual tasks.
Tweaking the resource request may be required when your tasks spawn additional actors or remote functions to avoid deadlock, see below.


Troubleshooting Common Resource Issues
--------------------------------------

My RLLib task fails
^^^^^^^^^^^^^^^^^^^

Due to the way Ray's RLLib works, a deadlock can be created on Compute Engine if a task attempts to spawn additional actors or remote functions via the DNQ ``rollouts`` facility. Resources requested in a task definition are bound to the task process, so additional actors can rapidly exhaust the provisioned resources.

In these cases, additional resources should be specified in the workflow decorator.

.. code-block::
    :caption: Example: override workflow resources.
    @sdk.task(resources=...)                    # task resources requested.
    def task():
        config = DQNConfig()
        ...
        config.rollouts(num_rollout_workers=2)  # additional actors do not have
        ...                                     # access to task resources.
        return results

    @sdk.workflow(resources=...)                # Override the aggregated task
    def wf():                                   # resources to provision additional
        results = []                            # resources for the additional
        for _ in range(5):                      # actors.
            results.append(task())

My Local Tasks Aren't Running
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Task resources are used to schedule tasks both locally and on remote runtimes.
This might lead to issues when running tasks locally if they require resources that are unavailable.

For example, you have a task that requires:

1. a GPU but during development you run the workflow on your laptop without a GPU.
2. 32GB of memory, but your Studio notebook only has 8GB available.
3. 16 CPU cores but your desktop only has 8 available.

In these examples, those tasks will not be scheduled by a local Ray instance due to the lack of resources.
To workaround this problem, you should reduce the resources to match what is available. This can be done in the decorator:

.. code-block:
    @sdk.task(resources=sdk.Resources(gpu="0"))
    def my_task():
        ...

or when the task is invoked, with the ``.with_resources()`` method:

.. code-block:
    # Usual request
    @sdk.task(resources=sdk.Resources(gpu="1"))
    def my_task():
        ...

    @sdk.workflow
    def my_workflow():
        # The resources are overridden for this one invocation
        result = my_task().with_resources(gpu="0")
        return result