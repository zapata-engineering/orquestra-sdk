from orquestra.workflow_shared import serde

import orquestra.sdk as sdk


def test_sdk_can_be_serialised():
    def sdk_pickle_by_ref():
        _ = sdk

    serde.serialize_pickle(sdk_pickle_by_ref)
