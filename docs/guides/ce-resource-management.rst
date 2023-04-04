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
* ``cpu``: number of cores.
* ``memory``: amount of RAM (bytes).
* ``disk``: disk space (bytes).
* ``gpu``: whether access to a gpu unit is required (``1`` if a GPU is required, ``0`` otherwise).

Amounts of cpu and memory resources can be specified as a plain integer, or as a fixed-point float appended with the string representation of one of the following unit prefixes. These can be SI (metric), or IEC (binary) prefixes. The specified prefix will be used to modify the base unit for the parameter, 'byte' in the case of ``memory`` and ``disk`` requests, 'cores' in the case of ``cpu`` requests. So ``disk="10k"`` will be interpreted as '10 kilobytes', while ``cpu="10k"`` would request 10^7 cores.

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

.. TODO: uncomment and check this section when workflow resource management is implemented (https://zapatacomputing.atlassian.net/browse/ORQSDK-797?atlOrigin=eyJpIjoiNGU1MDU0NjFhNTMxNGUwN2IyZTQzODMxZTVhNjQwM2UiLCJwIjoiaiJ9)

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

Due to the way Ray's RLLib works, a deadlock can be created on Compute Engine if a task attempts to spawn additional processes, notably via the DNQ ``rollouts`` facility. Resources requested in a task definition are bound to the task process, so additional processes can rapidly exhaust the provisioned resources.

.. TODO: uncomment and check this section when workflow resource management is implemented (https://zapatacomputing.atlassian.net/browse/ORQSDK-797?atlOrigin=eyJpIjoiNGU1MDU0NjFhNTMxNGUwN2IyZTQzODMxZTVhNjQwM2UiLCJwIjoiaiJ9)

    In these cases, additional resources should be specified in the workflow decorator.

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
