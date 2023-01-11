################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""Tests for the methods of the ArtifactFuture class
"""
import pytest

import orquestra.sdk as sdk
from orquestra.sdk import exceptions
from orquestra.sdk._base._dsl import DEFAULT_IMAGE

resources_no_default = {
    "cpu": "2000m",
    "memory": "100Gi",
    "disk": "99Gi",
    "gpu": "1",
}

custom_image_no_default = {
    "custom_image": "zapatacomputing/orquestra-not-default",
}

metadata_no_default = {**resources_no_default, **custom_image_no_default}


@sdk.task(resources=sdk.Resources(**resources_no_default))
def _resourced_task(ii: int = 0) -> int:
    return ii


@sdk.task
def _task_without_resources(ii: int = 0) -> int:
    return ii


class ObjWithTask:
    def __init__(self, ii: int = 0) -> None:
        self._ii = ii

    @sdk.task(resources=sdk.Resources(**resources_no_default))
    def get_id(
        self,
    ) -> int:
        return self._ii


def get_task(ind: bool = True):
    @sdk.task(resources=sdk.Resources(**resources_no_default))
    def task_get_number(number: int = 0):
        return number

    return task_get_number


@sdk.task
def two_ouputs_task():
    return 0, 1


@sdk.workflow
def wf_assign_resources_to_deconstructed_task_invocation():
    result1, result2 = two_ouputs_task()
    result1.with_resources(cpu=resources_no_default["cpu"])
    return [result1, result2]


@sdk.workflow
def wf_deconstruc_task_invocation_with_resources_assignment():
    result = two_ouputs_task()
    result1, result2 = result.with_resources(cpu=resources_no_default["cpu"])
    return [result1, result2]


@sdk.workflow
def wf_assign_custom_image_to_deconstructed_task_invocation():
    result1, result2 = two_ouputs_task()
    result1.with_custom_image(custom_image_no_default["custom_image"])
    return [result1, result2]


@sdk.workflow
def wf_deconstruc_task_invocation_with_custom_image_assignment():
    result = two_ouputs_task()
    result1, result2 = result.with_custom_image(custom_image_no_default["custom_image"])
    return [result1, result2]


@sdk.workflow
def wf_assign_metadata_to_deconstructed_task_invocation():
    result1, result2 = two_ouputs_task()
    result1.with_invocation_meta(
        custom_image=custom_image_no_default["custom_image"],
        gpu=resources_no_default["gpu"],
    )
    return [result1, result2]


@sdk.workflow
def wf_deconstruc_task_invocation_with_metadata_assignment():
    result = two_ouputs_task()
    result1, result2 = result.with_invocation_meta(
        custom_image=custom_image_no_default["custom_image"],
        gpu=resources_no_default["gpu"],
    )
    return [result1, result2]


@sdk.workflow
def wf_task_call():
    [_task_without_resources(), _resourced_task()]


@sdk.workflow
def wf_with_resources_call():
    future = _task_without_resources()
    new_future = future.with_resources(**resources_no_default)
    return [future, new_future]


@sdk.workflow
def wf_with_custom_image_call():
    future = _task_without_resources()
    new_future = future.with_custom_image(custom_image_no_default["custom_image"])
    return [future, new_future]


@sdk.workflow
def wf_with_invocation_meta_call():
    future = _task_without_resources()
    new_future = future.with_invocation_meta(**metadata_no_default)
    return [future, new_future]


@sdk.workflow
def wf_with_resources_and_custom_image_call():
    future = _task_without_resources()
    new_future = future.with_resources(**resources_no_default).with_custom_image(
        **custom_image_no_default
    )
    return [future, new_future]


@sdk.workflow
def wf_with_method_task():
    simple_obj = ObjWithTask(13)
    return [simple_obj.get_id()]


@sdk.workflow
def wf_with_mixed_calls():
    simple_obj = ObjWithTask(13)
    simple_obj.get_id()
    return _task_without_resources().with_invocation_meta(**metadata_no_default)


class TestArifactFutureMethodsCalls:
    """Test method calls of the ArtifactFuture and errors associated
    to incorrect calls to those methods. Also test the error generated
    when a task is a method of an object.
    """

    @staticmethod
    def test_task_call_with_metadata():
        """Check that the workflow model can be generated for
        workflows that implement correct calls to ArtifacFuture
        methods
        """
        wf_with_resources_call().model
        wf_with_invocation_meta_call().model
        wf_with_custom_image_call().model
        wf_task_call().model

    @staticmethod
    def test_task_method_call():
        """Check the proper formatting of the workflow syntax error
        associated to calling a task that is a method of an object
        """
        with pytest.raises(exceptions.WorkflowSyntaxError) as excinfo:
            wf_with_method_task().model
        print(str(excinfo.value))

        error_message = [
            "The task get_id seems to be a method, if so modify it to not be a method.",
            "Error message: missing a required argument: 'self'",
            "simple_obj.get_id()",
        ]
        assert all(message in str(excinfo.value) for message in error_message)

    @staticmethod
    def test_mixed_calls_worflow():
        """Check the proper formatting of the workflow syntax error
        associated to calling a task that is a method of an object
        """
        with pytest.raises(exceptions.WorkflowSyntaxError) as excinfo:
            wf_with_mixed_calls().model
        error_message = [
            "The task get_id seems to be a method, if so modify it to not be a method.",
            "Error message: missing a required argument: 'self'",
            "simple_obj.get_id()",
        ]
        assert all(message in str(excinfo.value) for message in error_message)

    @staticmethod
    def test_deconstruction_error_resource_assignment():
        """Check the proper formatting of the workflow syntax error
        associated to assigning resources to an ArtifactFuture that
        is desctructured
        """
        with pytest.raises(exceptions.WorkflowSyntaxError) as excinfo:
            wf_assign_resources_to_deconstructed_task_invocation().model
        error_message = [
            "Can't assign resources to an artifact that has been destructured.",
            "To assign resources to a call of the task two_ouputs_task make sure"
            " NOT to destructure its outputs.",
            ".with_resources(",
        ]
        assert all(message in str(excinfo.value) for message in error_message)

        wf_deconstruc_task_invocation_with_resources_assignment().model

    @staticmethod
    def test_deconstruction_error_custom_image_assignment():
        """Check the proper formatting of the workflow syntax error
        associated to assigning a custom image to an ArtifactFuture that
        is desctructured
        """
        with pytest.raises(exceptions.WorkflowSyntaxError) as excinfo:
            wf_assign_custom_image_to_deconstructed_task_invocation().model
        error_message = [
            "Can't assign a custom image to an artifact that has been destructured.",
            "To assign a custom image to a call of the task two_ouputs_task make sure"
            " NOT to destructure its outputs.",
            ".with_custom_image(",
        ]
        assert all(message in str(excinfo.value) for message in error_message)

        wf_deconstruc_task_invocation_with_custom_image_assignment().model

    @staticmethod
    def test_deconstruction_error_metadata_assignment():
        """Check the proper formatting of the workflow syntax error
        associated to assigning metadata to an ArtifactFuture that
        is desctructured
        """
        with pytest.raises(exceptions.WorkflowSyntaxError) as excinfo:
            wf_assign_metadata_to_deconstructed_task_invocation().model

        error_message = [
            "Can't assign invocation metadata to an artifact that has been "
            "destructured.",
            "To assign invocation metadata to a call of the task two_ouputs_task"
            " make sure"
            " NOT to destructure its outputs.",
            ".with_invocation_meta(",
        ]
        assert all(message in str(excinfo.value) for message in error_message)

        wf_deconstruc_task_invocation_with_metadata_assignment().model

    @staticmethod
    def test_artifact_with_metadata_workflow_model():
        """Check that the metadata assignment is done properly
        and there is a correct amount of tasks, task_invocations and
        artifact futures
        """
        wf_model = wf_with_invocation_meta_call().model
        assert len(wf_model.tasks) == 1
        assert len(wf_model.task_invocations) == 2
        assert len(wf_model.artifact_nodes) == 2
        assert (
            len(
                set(
                    invocation.task_id
                    for invocation in wf_model.task_invocations.values()
                )
            )
            == 1
        )
        assert [*wf_model.task_invocations.values()][0].resources is None
        assert [*wf_model.task_invocations.values()][
            1
        ].resources.dict() == resources_no_default

        assert [*wf_model.task_invocations.values()][0].custom_image is DEFAULT_IMAGE
        assert [*wf_model.task_invocations.values()][
            1
        ].custom_image == custom_image_no_default["custom_image"]

    @staticmethod
    def test_artifact_with_resources_workflow_model():
        """Check that the resources assignment is done properly
        and there is a correct amount of tasks, task_invocations and
        artifact futures
        """
        wf_model = wf_with_resources_call().model
        assert len(wf_model.tasks) == 1
        assert len(wf_model.task_invocations) == 2
        assert len(wf_model.artifact_nodes) == 2
        assert (
            len(
                set(
                    invocation.task_id
                    for invocation in wf_model.task_invocations.values()
                )
            )
            == 1
        )
        assert [*wf_model.task_invocations.values()][0].resources is None
        assert [*wf_model.task_invocations.values()][
            1
        ].resources.dict() == resources_no_default

        assert [*wf_model.task_invocations.values()][0].custom_image == [
            *wf_model.task_invocations.values()
        ][1].custom_image

    @staticmethod
    def test_artifact_with_custom_image_workflow_model():
        """Check that the custom image assignment is done properly
        and there is a correct amount of tasks, task_invocations and
        artifact futures
        """
        wf_model = wf_with_custom_image_call().model
        assert len(wf_model.tasks) == 1
        assert len(wf_model.task_invocations) == 2
        assert len(wf_model.artifact_nodes) == 2
        assert (
            len(
                set(
                    invocation.task_id
                    for invocation in wf_model.task_invocations.values()
                )
            )
            == 1
        )
        assert [*wf_model.task_invocations.values()][0].resources == [
            *wf_model.task_invocations.values()
        ][1].resources

        assert [*wf_model.task_invocations.values()][0].custom_image == DEFAULT_IMAGE
        assert [*wf_model.task_invocations.values()][
            1
        ].custom_image == custom_image_no_default["custom_image"]

    @staticmethod
    def test_artifact_with_resources_and_custom_image_workflow_model():
        """Check that the resources and custom mage assignment is done properly
        and there is a correct amount of tasks, task_invocations and
        artifact futures
        """
        wf_model = wf_with_invocation_meta_call().model
        assert len(wf_model.tasks) == 1
        assert len(wf_model.task_invocations) == 2
        assert len(wf_model.artifact_nodes) == 2
        assert (
            len(
                set(
                    invocation.task_id
                    for invocation in wf_model.task_invocations.values()
                )
            )
            == 1
        )
        assert [*wf_model.task_invocations.values()][0].resources is None
        assert [*wf_model.task_invocations.values()][
            1
        ].resources.dict() == resources_no_default

        assert [*wf_model.task_invocations.values()][0].custom_image is DEFAULT_IMAGE
        assert [*wf_model.task_invocations.values()][
            1
        ].custom_image == custom_image_no_default["custom_image"]
