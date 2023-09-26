################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
import typing as t
from copy import copy

from orquestra.sdk.schema import ir

Node = str

# Graph uses dict with Nones instead of set because dict is ordered, and set is not
# and we need the order to be deterministic
Graph = t.Dict[Node, t.Dict[Node, None]]


def _invert_graph(graph: Graph) -> Graph:
    inverted: Graph = {}
    for node, deps in graph.items():
        for dep in deps:
            # Graph uses dict with Nones instead of set because dict is ordered
            inverted.setdefault(dep, dict())[node] = None
    return inverted


def _root_nodes(graph: Graph) -> t.Dict[Node, None]:
    all_nodes = {v: None for node, deps in graph.items() for v in [node, *deps]}
    needed_nodes = {dep: None for deps in graph.values() for dep in deps}
    # return all_nodes - needed_nodes
    return {v: None for v in all_nodes.keys() if v not in needed_nodes}


def topological_sort(graph_to_sort: Graph) -> t.List[Node]:
    """Returns a flat list of nodes that reflect walking over nodes without ever moving
    against the arrow in the DAG.

    Implements Kahn's algorithm. See listing at:
    https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm

    This function replicates the functionality of `graphlib`, but it's only available
    since Python 3.9.

    Args:
        graph_to_sort: key – node of interest, value – successors. The key needs to be
            visited before any of the successors.

    Raises:
        ValueError: When the nodes cannot be sorted because there is a least one cycle
            in the graph.
    """  # noqa: D205
    # We need a local copy because Kahn's algorithm mutates data.
    graph = {node: copy(successors) for node, successors in graph_to_sort.items()}

    # We'll need this to check if a node has any incoming edges/dependencies (line 9 in
    # the algorithm).
    inverted_graph = _invert_graph(graph)

    # List that will contain the sorted elements.
    L = []
    # Set of all nodes with no incoming edge.
    S = _root_nodes(graph)

    while S:
        n = S.popitem()[0]
        L.append(n)
        n_deps = list(graph.get(n, []))
        for m in n_deps:
            graph[n].pop(m)

            node = inverted_graph.get(m)
            # <m> should always be a member of inverted_graph
            assert node is not None
            node.pop(n)

            if node == {}:
                # Graph uses dict with Nones instead of set because dict is ordered
                S[m] = None

    for deps in graph.values():
        if deps:
            raise ValueError("Graph has at least one cycle")

    return L


def iter_invocations_topologically(wf: ir.WorkflowDef):
    # The graph is like: [I need this node first]->[successors].
    graph: Graph = {}
    for invocation in wf.task_invocations.values():
        for arg_id in [*invocation.args_ids, *invocation.kwargs_ids.values()]:
            # We need the argument before we can run the invocation
            # Graph uses dict with Nones instead of set because dict is ordered
            graph.setdefault(arg_id, dict())[invocation.id] = None

        for output_id in invocation.output_ids:
            # We need to run the invocation before we can have the output
            # Graph uses dict with Nones instead of set because dict is ordered
            graph.setdefault(invocation.id, dict())[output_id] = None

    sorted_node_ids = topological_sort(graph)

    for node_id in sorted_node_ids:
        # The DAG nodes are constants, artifacts, and invocations. Here, we only want
        # the invocations.
        if node_id in wf.task_invocations:
            yield wf.task_invocations[node_id]
