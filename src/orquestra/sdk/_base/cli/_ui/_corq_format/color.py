################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import pydantic
import pygments
import pygments.formatters
import pygments.lexers.data


def print_colorized_json(model: pydantic.BaseModel):
    """
    Low effort human-readable formatting: colorized, pretty-printed JSON.
    """
    print(
        pygments.highlight(
            model.json(indent=2),
            pygments.lexers.data.JsonLexer(),
            pygments.formatters.TerminalFormatter(),
        )
    )
