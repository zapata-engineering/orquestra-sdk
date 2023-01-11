################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
import typing as t

from orquestra.sdk.schema import ir

Node = str
Graph = t.Dict[Node, t.Set[Node]]


def _invert_graph(graph: Graph) -> Graph:
    inverted: Graph = {}
    for node, deps in graph.items():
        for dep in deps:
            inverted.setdefault(dep, set()).add(node)
    return inverted


def _root_nodes(graph: Graph) -> t.Set[Node]:
    all_nodes = {v for node, deps in graph.items() for v in [node, *deps]}
    needed_nodes = {dep for deps in graph.values() for dep in deps}
    return all_nodes - needed_nodes


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
    """
    # We need a local copy because Kahn's algorithm mutates data.
    graph = {node: set(successors) for node, successors in graph_to_sort.items()}

    # We'll need this to check if a node has any incoming edges/dependencies (line 9 in
    # the algorithm).
    inverted_graph = _invert_graph(graph)

    # List that will contain the sorted elements.
    L = []
    # Set of all nodes with no incoming edge.
    S = _root_nodes(graph)

    while S:
        n = S.pop()
        L.append(n)
        n_deps = list(graph.get(n, []))
        for m in n_deps:
            graph[n].remove(m)
            inverted_graph.get(m, set()).remove(n)

            if not inverted_graph.get(m):
                S.add(m)

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
            graph.setdefault(arg_id, set()).add(invocation.id)

        for output_id in invocation.output_ids:
            # We need to run the invocation before we can have the output
            graph.setdefault(invocation.id, set()).add(output_id)

    sorted_node_ids = topological_sort(graph)

    for node_id in sorted_node_ids:
        # The DAG nodes are constants, artifacts, and invocations. Here, we only want
        # the invocations.
        if node_id in wf.task_invocations:
            yield wf.task_invocations[node_id]
