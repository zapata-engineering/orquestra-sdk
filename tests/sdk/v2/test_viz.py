################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
from orquestra.sdk._base import _viz

from . import sample_wfs


class TestWfDefGraph:
    """
    Test boundary: [ir.WorkflowDef]-[_viz.DiGraph]
    """

    @staticmethod
    def test_example_nodes_edges():
        """ """
        wf_def = sample_wfs.wf(123).model
        graph = _viz.wf_def_graph(wf_def)

        assert graph.nodes1 == [
            _viz.Node(
                id="invocation-0-task-add",
                caption=["tests.sdk.v2.sample_wfs.add():12", "invocation-0-task-add"],
            ),
            _viz.Node(
                id="invocation-1-task-integer-division",
                caption=[
                    "tests.sdk.v2.sample_wfs.integer_division():27",
                    "invocation-1-task-integer-division",
                ],
            ),
            _viz.Node(
                id="invocation-2-task-inc",
                caption=["tests.sdk.v2.sample_wfs.inc():17", "invocation-2-task-inc"],
            ),
            _viz.Node(
                id="invocation-3-task-inc-2",
                caption=["[inlined].inc_2()", "invocation-3-task-inc-2"],
            ),
        ]
        assert graph.nodes2 == [
            _viz.Node(id="artifact-0-add", caption=[]),
            _viz.Node(id="artifact-1-integer-division", caption=[]),
            _viz.Node(id="artifact-4-integer-division", caption=[]),
            _viz.Node(id="artifact-2-inc", caption=[]),
            _viz.Node(id="artifact-3-inc-2", caption=[]),
            _viz.Node(id="constant-0", caption=["6"]),
            _viz.Node(id="constant-3", caption=["3"]),
            _viz.Node(id="constant-4", caption=["123"]),
        ]
        assert graph.edges == [
            ("artifact-1-integer-division", "invocation-0-task-add"),
            ("constant-0", "invocation-0-task-add"),
            ("invocation-0-task-add", "artifact-0-add"),
            ("constant-4", "invocation-1-task-integer-division"),
            ("constant-3", "invocation-1-task-integer-division"),
            ("invocation-1-task-integer-division", "artifact-1-integer-division"),
            ("invocation-1-task-integer-division", "artifact-4-integer-division"),
            ("artifact-3-inc-2", "invocation-2-task-inc"),
            ("invocation-2-task-inc", "artifact-2-inc"),
            ("artifact-4-integer-division", "invocation-3-task-inc-2"),
            ("invocation-3-task-inc-2", "artifact-3-inc-2"),
        ]
