################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
import pytest

from orquestra.sdk._base import _graphs


class TestTopologicalSort:
    @pytest.mark.parametrize(
        "graph,order",
        [
            (
                #  a─┬───┬►b─►d
                #    └►c─┘
                {
                    "a": {"b": None, "c": None},
                    "b": {"d": None},
                    "c": {"b": None},
                },
                ["a", "c", "b", "d"],
            ),
        ],
    )
    def test_simple_examples(self, graph, order):
        assert _graphs.topological_sort(graph) == order

    @pytest.mark.parametrize(
        "graph",
        [
            #  a─┬───┬►b─►d
            #    └►c─┘
            {
                "a": {"b": None, "c": None},
                "b": {"d": None},
                "c": {"b": None},
            },
            #  a─┬───┬►b─►d
            #  e─┴►c─┴►f
            {
                "a": {"b": None, "c": None, "f": None},
                "b": {"d": None},
                "c": {"b": None, "f": None},
                "e": {"b": None, "c": None, "f": None},
            },
        ],
    )
    def test_successors_after_predecessors(self, graph):
        sorted_nodes = _graphs.topological_sort(graph)

        node_indices = {node: node_i for node_i, node in enumerate(sorted_nodes)}

        for node, successors in graph.items():
            for successor in successors:
                assert node_indices[successor] > node_indices[node]
