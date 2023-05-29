Using Custom Docker Images on Compute Engine
=====================

When running workflows remotely on Compute Engine, one of the ``orquestra-sdk-base`` images are used
depending on whether you have requested a GPU or not (you can see their source code
`here <https://github.com/zapatacomputing/orquestra-workflow-sdk/blob/main/docker/Dockerfile>`_ and
`here <https://github.com/zapatacomputing/orquestra-workflow-sdk/blob/main/docker/cuda.Dockerfile>`_).
If you have dependencies that you need to have installed that you cannot install using imports (such
as native libraries or executable binaries), you can use one of the above images as your base image to
publish a new image that you will use as your container when running the workflow.

Building an Image
-----------------

The base images are published to our Nexus Docker registry. It follows the pattern
``hub.nexus.orquestra.io/zapatacomputing/orquestra-sdk-base:<SDK version>[-cuda]``. For example,
``hub.nexus.orquestra.io/zapatacomputing/orquestra-sdk-base:0.48.0`` or
``hub.nexus.orquestra.io/zapatacomputing/orquestra-sdk-base:0.49.0-cuda``. You should pick the one that matches
your SDK version and your GPU needs (``-cuda`` needs to be picked for GPU workflows) to be your base image.

The base images run as a user named ``orquestra`` with uid ``1000``. Since these are Ubuntu based images, you
can install any package that you need from Ubuntu repositories (by doing ``RUN apt install <package name>``).
However, before doing so, you need to temporarily switch to the root user (via ``USER root`` directive) and
restore back to user ``orquestra`` once you're done (``USER orquestra``).

You can use `standard OCI annotations <https://github.com/opencontainers/image-spec/blob/main/annotations.md>`_
to add metadata to your images.

In order for the Compute Engine to run your workflow code, you should not have a ``ENTRYPOINT`` or ``CMD`` statement in
your images. In case you do, these will be overriden at run time and you might observe unexpected behavior.

Publishing the Image
--------------------

Once you have your ``Dockerfile`` ready, you can publish your image to our Nexus repository by using standard Docker tools.
Please refer to `this page <https://zapatacomputing.atlassian.net/wiki/spaces/~61209e4528ae75006af8a1b8/pages/619577422/Nexus+Starts+Here>`_
to see how.

..
    TODO: Either move the page to a more general space or copy the relevant bits here


Using the Image on Compute Engine
---------------------------------

You can run workflows that use custom images only on Compute Engine and for that you need to request more than
one node in your workflow resources. That's because a new container needs to be started for each image you specify plus
one that has the default image and runs tasks which don't require a custom image.

Speaking of workflow resources, you also need to make sure that your workflow resources specify a value that is typically
20 percent larger than the largest value you use for any task run. For example, if you have requested `cpu=1000m` for
one of your task resources, you should specify `cpu=1200m` as your workflow resources.

When you use a custom image, ``nodes`` workflow resource becomes the maximum number of nodes that will be created per
unique Docker image you use. For example, if you use two different custom images for your tasks and specify ``nodes=5``
in your workflow resources, a maximum of 15 nodes will be created (note the addition of default image). As in the case
without any custom images, containers will get created and destroyed based on resource requests from tasks.

To make your task use a custom image on Compute Engine, you need to pass a ``custom_image`` argument to the
``@sdk.task()`` decorator as shown below:

.. code-block::
    :caption: Custom image example

    @sdk.task(
        custom_image="hub.nexus.orquestra.io/users/emre-aydin/my-custom-image:1.2.3"
    )
    def train_model(x, y) -> LinearRegression:
        ...
