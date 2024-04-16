################################################################################
# © Copyright 2022 - 2023 Zapata Computing Inc.
################################################################################
import ast
import logging
import re
import typing as t
from enum import Enum

LOG = logging.getLogger(__name__)


class _AstReturnMetadata(t.NamedTuple):
    is_subscriptable: bool
    n_outputs: int


class OutputCounterVisitor(ast.NodeVisitor):
    """Counts how many "outputs" a task function has.

    Activates only on "Return" and "Pass" statements.

    This is the "outer" visitor. Defers most of the analysis to the "inner"
    visitor.
    The idea is to parse _any_ tree are nested _under_ a return clause.
    """

    def __init__(self):
        self._expr_visitor = _ReturnExprVisitor()

    @property
    def outputs(self) -> t.Set[_AstReturnMetadata]:
        return self._expr_visitor.outputs

    def _handle_empty_output(self):
        # Empty return statements result in a single "None" output value.
        # Every task needs to return at least a single artifact, even if
        # its value will resolve to "None" at run-time. Otherwise, the task
        # would never be invoked, as the worklflow def parser is lazy.
        self._expr_visitor.outputs.add(
            _AstReturnMetadata(is_subscriptable=False, n_outputs=1)
        )

    def visit_Return(self, node):
        if node.value is None:
            self._handle_empty_output()
        else:
            self._expr_visitor.visit(node.value)

    def visit_Pass(self, node):
        self._handle_empty_output()


class _ReturnExprVisitor(ast.NodeVisitor):
    """The "inner" visitor.

    Handles any expression that's nested under the return statement.
    """

    def __init__(self):
        self.outputs: t.Set[_AstReturnMetadata] = set()

    def _fail_single(
        self,
        node: ast.AST,
        message: str,
    ):
        LOG.debug(message)
        self._add_single()

    def _add_single(self):
        self.outputs.add(_AstReturnMetadata(is_subscriptable=False, n_outputs=1))

    def _add_sequence(self, node, elements: t.List[ast.expr]):
        # If there is a starred node in the list, then unpacking is used
        if any(isinstance(e, ast.Starred) for e in elements):
            self._fail_single(node, "Cannot infer number of outputs with unpacking")
            return

        n_elements = len(elements)
        if n_elements == 0:
            # Empty list counts as a single output, similarly to void functions
            # having a single `None` output.
            self._add_single()
        else:
            self.outputs.add(
                _AstReturnMetadata(is_subscriptable=True, n_outputs=n_elements)
            )

    def visit_Constant(self, node):
        self._add_single()

    def visit_Name(self, node):
        self._add_single()

    def visit_Attribute(self, node):
        self._add_single()

    def visit_JoinedStr(self, node):
        # f-strings
        self._add_single()

    def visit_Set(self, node):
        # Sets are python objects and are not subscriptable
        self._add_single()

    def visit_Tuple(self, node):
        self._add_sequence(node, node.elts)

    def visit_List(self, node):
        self._add_sequence(node, node.elts)

    def visit_Dict(self, node):
        # "None" inside the keys means there is dictionary unpacking being used
        # If None is used as a key, it is represented by an AST node
        if None in node.keys:
            self._fail_single(node, "Cannot infer number of outputs with unpacking")
            return

        self._add_single()

    def generic_visit(self, node):
        self._fail_single(node, f"Assuming a single output for node {repr(type(node))}")


def normalize_indents(source: str) -> str:
    lines = source.splitlines()
    match = re.match(r"(\s+)", lines[0])
    if match is not None:
        indent = len(match.groups()[0])
    else:
        indent = 0
    lines_joined = "\n".join(line[indent:] for line in lines)
    # `.splitlines()` removes the trailing newline
    return lines_joined + "\n"


class NodeReferenceType(Enum):
    """Types of ast nodes that are relevant to analyze a Call."""

    ATTRIBUTE = "ATTRIBUTE"
    NAME = "NAME"
    CALL = "CALL"


class NodeReference(t.NamedTuple):
    """Reference to a Node in the workflow function's AST."""

    name: str
    node_type: NodeReferenceType


class _Call(t.NamedTuple):
    """Call identified by the CallVisitor.

    Args:
        callable_name: Dummy object with the name of the callable.
        call_statement: list of modules or calls used to
            perform the call.
        line_no: line where the callable is call inside the
            workflow definition.
    """

    callable_name: str

    call_statement: t.List[NodeReference] = []
    line_no: t.Optional[int] = None


def get_call_statement(node: ast.Call) -> t.List[NodeReference]:
    """Get each part of a call.

    Examples:
        "task()" → [task()]
        "sdk.task()" → [sdk, task()]
        "task().with_resources()" → [task(),with_resources()]

    Args:
        node: AST node to check the name.

    Returns:
        A list with each part of the name separated out.
    """
    current_node: ast.AST = node
    call_statement: t.List[NodeReference] = []
    while isinstance(current_node, ast.Attribute) or isinstance(current_node, ast.Call):
        if isinstance(current_node, ast.Attribute):
            call_statement.insert(
                0,
                NodeReference(
                    name=current_node.attr, node_type=NodeReferenceType.ATTRIBUTE
                ),
            )
            current_node = current_node.value
        else:
            if isinstance(current_node.func, ast.Name):
                # The Node is a name of the workflow
                call_statement.insert(
                    0,
                    NodeReference(
                        name=current_node.func.id, node_type=NodeReferenceType.CALL
                    ),
                )
                break
            elif isinstance(current_node.func, ast.Attribute):
                # Node is an attribute
                call_statement.insert(
                    0,
                    NodeReference(
                        name=current_node.func.attr, node_type=NodeReferenceType.CALL
                    ),
                )
                current_node = current_node.func.value
            elif isinstance(current_node.func, ast.Call):
                call_statement.insert(
                    0,
                    NodeReference(name="", node_type=NodeReferenceType.CALL),
                )
                current_node = current_node.func
            else:
                # We do not support other cases
                break
    if isinstance(current_node, ast.Name):
        call_statement.insert(
            0,
            NodeReference(name=current_node.id, node_type=NodeReferenceType.NAME),
        )
    return call_statement


class CallVisitor(ast.NodeVisitor):
    """Node visitor that focus on identifying calls."""

    def __init__(self) -> None:
        self.calls: t.List[_Call] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Visits a function definition by only iterating over its statements.

        Other information about the Function definition is not analyzed e.g.
        decorators.

        Args:
            node: ast node of type FunctionDef.
        """
        for statement in node.body:
            self.generic_visit(statement)

    def visit_Call(self, node: ast.Call) -> None:
        """Extract information of a Call.

        Args:
            node: node for a Call in a AST.

        Returns:
            _Call object with information about the Call statement.
        """
        call_statement = get_call_statement(node)
        self.calls.append(
            _Call(
                call_statement=call_statement,
                callable_name=call_statement[-1].name,
                line_no=node.lineno,
            )
        )
