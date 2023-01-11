Runtime Configuration
=====================

.. decide where to expose this in the docs

Orquestra can support different execution environments for workflows, called
runtimes. Currently there are two supported runtimes: local execution and
remote execution via Quantum Engine.

In some cases, additional configuration options are required in order to use a
runtime. For example, a URL is required to connect to Quantum Engine. Choosing
a runtime and supplying options is called a *Runtime Configuration*.

There is always one *Runtime Configuration* defined, called ``local``. This
configuration option is reserved and cannot be updated or saved to. Manually
editing the configuration file will not change this reserved option.

This is used by default in the CLI and executes a workflow locally with the
default runtime options.

..
    TODO: Add how CLI uses configurations

Configuration File
------------------

These *Runtime Configurations* are stored in a configuration file located at
``~/.orquestra/config.json``. The configuration file is a JSON file that is
defined by:

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Option
     - Description
   * - ``version``
     - The version of the runtime configuration schema currently in use.
   * - ``configs``
     - Key-value pairs where the key is the configuration name and the value
       is the ``RuntimeConfiguration``. See RuntimeConfiguration_.


.. _RuntimeConfiguration:

``RuntimeConfiguration``
------------------------

Inside the configuration file, each *Runtime Configuration* is defined by:

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Configuration option
     - Description
   * - ``config_name``
     - The human readable configuration name. This is what you should use in
       the CLI to reference a configuration.
   * - ``runtime_name``
     - The internal reference to the Orquestra runtime. Currently supported
       options: ``RAY_LOCAL``, ``QE_REMOTE``.
   * - ``runtime_options``
     - A key-value pair of options passed to the specific runtime.
