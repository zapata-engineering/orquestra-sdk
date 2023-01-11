################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import io
import json
import tarfile

import pytest

from orquestra.sdk._base._qe._qe_runtime import extract_result_json

WORKFLOW_RESULT_JSON_DICT = {
    "hello-there-abc123-2738763496": {
        "artifact-1-multi-output-test": {
            "serialization_format": "JSON",
            "value": '"there"',
        },
        "inputs": {
            "__sdk_fn_ref_dict": {
                "type": "sdk-metadata",
                "value": {
                    "file_path": "git-1871174d6e_github_com_zapatacomputing_orquestra_sdk/src/orquestra/sdk/examples/exportable_wf.py",  # noqa: E501
                    "function_name": "multi_output_test",
                    "line_number": 29,
                    "type": "FILE_FUNCTION_REF",
                },
            },
            "__sdk_output_node_dicts": {
                "type": "sdk-metadata",
                "value": [
                    {
                        "artifact_index": 1,
                        "custom_name": None,
                        "id": "artifact-1-multi-output-test",
                        "serialization_format": "AUTO",
                    }
                ],
            },
            "__sdk_positional_args_ids": {"type": "sdk-metadata", "value": []},
        },
        "stepID": "hello-there-abc123-2738763496",
        "stepName": "invocation-1-task-multi-output-test-86ee464194",
        "workflowId": "hello-there-abc123",
    },
    "hello-there-abc123-3825957270": {
        "artifact-0-make-greeting": {
            "serialization_format": "JSON",
            "value": '"hello, alex zapata!there"',
        },
        "inputs": {
            "__sdk_fn_ref_dict": {
                "type": "sdk-metadata",
                "value": {
                    "file_path": "git-1871174d6e_github_com_zapatacomputing_orquestra_sdk/src/orquestra/sdk/examples/exportable_wf.py",  # noqa: E501
                    "function_name": "make_greeting",
                    "line_number": 19,
                    "type": "FILE_FUNCTION_REF",
                },
            },
            "__sdk_output_node_dicts": {
                "type": "sdk-metadata",
                "value": [
                    {
                        "artifact_index": None,
                        "custom_name": None,
                        "id": "artifact-0-make-greeting",
                        "serialization_format": "AUTO",
                    }
                ],
            },
            "__sdk_positional_args_ids": {"type": "sdk-metadata", "value": []},
            "additional_message": {
                "sourceArtifactName": "artifact-1-multi-output-test",
                "sourceStepID": "hello-there-abc123-2738763496",
            },
            "first": {
                "type": "workflow-result-dict",
                "value": {"serialization_format": "JSON", "value": '"alex"'},
            },
            "last": {
                "type": "workflow-result-dict",
                "value": {"serialization_format": "JSON", "value": '"zapata"'},
            },
        },
        "stepID": "hello-there-abc123-3825957270",
        "stepName": "invocation-0-task-make-greeting-f0170bf7bf",
        "workflowId": "hello-there-abc123",
    },
}


def test_empty_bytes():
    empty = b""
    with pytest.raises(tarfile.ReadError):
        extract_result_json(empty)


def test_invalid_tgz_bytes():
    invalid = b""
    with pytest.raises(tarfile.ReadError):
        extract_result_json(invalid)


def test_valid_tgz_empty():
    tarfile_obj = io.BytesIO()
    tar = tarfile.open(fileobj=tarfile_obj, mode="w:gz")
    tar.close()
    tarfile_obj.seek(0)
    with pytest.raises(IndexError):
        extract_result_json(tarfile_obj.getbuffer())


def test_valid_tgz_valid_json():
    content_str = json.dumps(WORKFLOW_RESULT_JSON_DICT)
    content_file = io.BytesIO()
    content_file.write(content_str.encode())
    content_file.seek(0)
    content_info = tarfile.TarInfo(name="results.json")
    content_info.size = len(content_str)
    tarfile_obj = io.BytesIO()
    tar = tarfile.open(fileobj=tarfile_obj, mode="w:gz")
    tar.addfile(tarinfo=content_info, fileobj=content_file)
    tar.close()
    tarfile_obj.seek(0)
    result = extract_result_json(tarfile_obj.getbuffer())
    assert result == WORKFLOW_RESULT_JSON_DICT
