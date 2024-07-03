==========
Installing
==========

This tutorial explains how to install the Orquestra Workflow SDK on macOS and Linux.
For installation instruction for Windows, see: :doc:`Installing on Windows <installing-windows>`.


Install Orquestra Workflow SDK
==============================

Install the Orquestra Workflow SDK by running:

.. code-block:: bash

    pip install "orquestra-sdk[all]"

This will install ``orquestra-sdk`` and all of its dependencies.
Note that ``orquestra-sdk`` also includes the :doc:`Orquestra command-line interface <../quickref/cli-reference>`.

Note:

.. code-block:: rst

    When running the command above, you may observe an issue with it trying to install both 0.64.0 and 0.63.0
    with dependency conflicts error of 'ray==2.9.0; extra == "all"'. 
    Focus on installing the 0.64.0 version and make sure your Python environment is 3.11.x 
    as this is the go-to version used by the team that also supports Ray 2.9.0

    For this a virtual environment is highly recommended.


Apple Silicon
=============

When running on M1-like CPUs you might see errors like::

    ImportError: Failed to import grpc on Apple Silicon. On Apple Silicon machines, try
    `pip uninstall grpcio; conda install grpcio`. Check out https://docs.ray.io/en/master/ray-overview/installation.html#m1-mac-apple-silicon-support
    for more details.

It's related to one of Orquestra Workflow SDK's transitive dependencies.
Contrary to the error's message, combining ``pip`` and ``conda`` is not advised.
Instead, you can work around the error with the following:


.. code:: bash

    pip uninstall grpcio; pip install grpcio==1.43.0 --no-binary :all:
