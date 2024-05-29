################################################################################
# © Copyright 2022 - 2023 Zapata Computing Inc.
################################################################################
"""Workflow visualization using graphviz."""

import itertools
import typing as t
from dataclasses import dataclass

import graphviz  # type: ignore
from orquestra.workflow_shared.schema import ir

NodeId = str


@dataclass(frozen=True)
class Node:
    id: NodeId
    caption: t.Sequence[str]


Edge = t.Tuple[NodeId, NodeId]


@dataclass(frozen=True)
class BiGraph:
    """Bipartite graph with two groups of nodes. Renderer-agnostic."""

    nodes1: t.Sequence[Node]
    nodes2: t.Sequence[Node]
    edges: t.Sequence[Edge]


def wf_def_graph(wf_def: ir.WorkflowDef) -> BiGraph:
    """Builds a bipartite graph from the workflow def.

    The output is renderer-agnostic, but it's close to the shape we pass to graphviz.

    The "logic" layer.
    All the data we want to put on the visualization should be generated here.
    """
    task_nodes: t.List[Node] = []
    artifact_nodes: t.List[Node] = []
    edges: t.List[Edge] = []

    # for task_run in wf_status.task_runs:
    for task_inv in wf_def.task_invocations.values():
        task_def = wf_def.tasks[task_inv.task_id]

        task_location: str
        line_no_suffix: str
        if isinstance(task_def.fn_ref, ir.ModuleFunctionRef):
            task_location = task_def.fn_ref.module

            if line_no := task_def.fn_ref.line_number:
                line_no_suffix = f":{line_no}"
            else:
                line_no_suffix = ""
        elif isinstance(task_def.fn_ref, ir.InlineFunctionRef):
            task_location = "[inlined]"
            line_no_suffix = ""
        else:
            raise ValueError(f"Unsupported fn_ref: {task_def.fn_ref}")

        fn_name = task_def.fn_ref.function_name

        # In the graph, each task invocation is a cluster of [input artifacts]
        # + [task node] + [output artifacts].
        #
        # We use task_run_id as the function node IDs, and artifact/constant IDs for
        # artifact node IDs.

        # 1. Add task node.
        task_nodes.append(
            Node(
                id=task_inv.id,
                caption=[
                    f"{task_location}.{fn_name}(){line_no_suffix}",
                    task_inv.id,
                ],
            )
        )

        # 2. Connect task with arguments
        for arg_id in itertools.chain(task_inv.args_ids, task_inv.kwargs_ids.values()):
            # We don't define a node here, because artifacts are declared with the
            # tasks that produce them (below), and workflow constants are defined
            # totally separately.
            edges.append((arg_id, task_inv.id))

        # 3. Connect task with outputs
        for artifact_id in task_inv.output_ids:
            artifact_nodes.append(Node(id=artifact_id, caption=[]))
            edges.append((task_inv.id, artifact_id))

    # 4. Add workflow constants
    for constant in wf_def.constant_nodes.values():
        artifact_nodes.append(Node(id=constant.id, caption=[constant.value_preview]))

    aggregation_step_id = "aggregation_step"
    artifact_nodes.append(Node(id=aggregation_step_id, caption=["outputs"]))
    for wf_output in wf_def.output_ids:
        edges.append((wf_output, aggregation_step_id))

    return BiGraph(nodes1=task_nodes, nodes2=artifact_nodes, edges=edges)


def translate_to_graphviz(graph: BiGraph) -> graphviz.Digraph:
    """Builds a graphviz object based on the renderer-agnostic bigraph.

    The "presentation" layer.
    Any styling should be done here.
    """
    digraph = graphviz.Digraph()
    # Make the plot a little less eye gouging.
    digraph.attr("node", fontname="courier")
    digraph.attr("node", fontcolor="gray7")
    digraph.attr("edge", fontcolor="gray7")

    for node in graph.nodes1:
        digraph.node(name=node.id, label="\n".join(node.caption), shape="rectangle")

    for node in graph.nodes2:
        if node.caption:
            # Normally we'd use circles for any data node, but this looks bad if the
            # node has text.
            shape = "Mrecord"
        else:
            shape = "circle"
        digraph.node(name=node.id, label="\n".join(node.caption), shape=shape)

    for src_id, dest_id in graph.edges:
        digraph.edge(src_id, dest_id)

    return digraph


def wf_def_to_graphviz(wf_def: ir.WorkflowDef) -> graphviz.Digraph:
    """Helper, combines the "logic" and "presentation" layers."""
    return translate_to_graphviz(wf_def_graph(wf_def))
