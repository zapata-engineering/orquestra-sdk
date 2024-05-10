Runtime Configuration
=====================

Most productive things you can do with Orquestra Workflow SDK require interacting with runtimes.
A *runtime* is execution environment for workflows; it knows about your workflow runs, can host web UIs, and stores Orquestra secrets.

Runtimes fall into two general categories:

#. *Remote*.
   Orquestra Compute Engine.
   Managed clusters.
#. *Local*.
   These runtimes support a subset of Orquestra functionality for local use.

Runtime Selection
-----------------

Using many of the Workflow SDK's APIs requires that you first specify which runtime to use.
Runtimes are identified by **config name**:

* ``auto`` -- infers the runtime to use based on contextual information.
  See the section below for more information.
* ``in_process`` -- refers to a local runtime that executes tasks sequentially in the same Python process. Useful for debugging.
* ``ray`` -- refers to a local Ray runtime that executes workflows in the background.
  Allows running tasks in parallel.
  Requires starting the runtime daemon with ``orq up``.
* ``local`` -- alias for ``ray``.
  Created at a time when there was just a single local runtime kind.
* **other config name** -- refers to a remote runtime.
  Most of the remote runtime URIs have the form ``https://<subdomain>.<domain>.<tld>``, we use the ``<subdomain>`` as the identifying config name.
  Connection details for remote runtimes are stored in ``~/.orquestra/config.json``.


``auto`` Config Resolution
--------------------------

.. list-table::
   :widths: 50 50
   :header-rows: 1

   * - Scenario
     - Resolved Runtime
   * - You're in Orquestra Studio (the web IDE you can access by visiting Orquestra cluster URI in your web browser) -- either your code in a Jupyter cell or a terminal prompt.
     - The same Orquestra cluster as the Studio instance.
   * - Code inside a task being executed by Compute Engine (the remote runtime).
     - The same Orquestra cluster as Compute Engine.
   * - Code in a script running on your local machine.
     - Contents of the ``ORQ_CURRENT_CONFIG`` environment variable, or error if this variable is unset.
