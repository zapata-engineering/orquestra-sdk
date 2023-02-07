==========
Installing
==========

This tutorial explains how to install the Orquestra Workflow SDK on macOS and Linux.
For installation instruction for Windows, see: :doc:`Installing on Windows <installing-windows>`.

.. only:: internal

  Access to Nexus
  ===============
  If you are a Zapata employee, you can install the Workflow SDK via Nexus.


    .. note::
        You can skip this step if you are a Zapata employee and you already have an Orquestra account. Your Orquestra account will give you access to Zapata's Nexus repository. If you are a Zapata employee without an Orquestra account, you can request an account through the `Zapata Cloud Support portal <https://zapatacomputing.atlassian.net/servicedesk/customer/portal/4/group/14/create/35>`_.

  Update your pip configuration file
  ==================================
  Add the lines provided by Zapata to your `pip configuration file <https://pip.pypa.io/en/stable/topics/configuration/#configuration-files>`_. (You will need to create one if it does not exist already.)


      For Zapata internal, use these lines

      .. code-block::

          [global]
          index = https://nexus.orquestra.wtf/repository/pypi/pypi
          index-url = https://nexus.orquestra.wtf/repository/pypi/simple


  To verify your configuration is correct run ``pip config list -v``. That will print the configuration being used, and the source configuration files it came from.


Install Orquestra Workflow SDK
==============================

Install the Orquestra Workflow SDK by running:

.. code-block:: bash

    pip install orquestra-sdk[all]

.. only:: internal

  If you're using Nexus, enter the email address and password associated with your orquestra.io account when prompted.

    .. note::
        For Zapata employees, your Orquestra password might be different from the Okta single sign-on password you use for corporate applications.

This will install ``orquestra-sdk`` and all of its dependencies. Note that ``orquestra-sdk`` also includes the :doc:`Orquestra command-line interface <../quickref/cli-reference>`.


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
