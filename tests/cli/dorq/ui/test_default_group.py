# Copyright (c) 2015, Heungsub Lee
# All rights reserved.

# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:

#   Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.

#   Redistributions in binary form must reproduce the above copyright notice, this
#   list of conditions and the following disclaimer in the documentation and/or
#   other materials provided with the distribution.

#   Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import click
import pytest
from click.testing import CliRunner

from orquestra.sdk._base.cli._dorq._ui._click_default_group import DefaultGroup


@click.group(cls=DefaultGroup, default="foo", invoke_without_command=True)
@click.option("--group-only", is_flag=True)
def cli(group_only):
    # Called if invoke_without_command=True.
    if group_only:
        click.echo("--group-only passed.")


@cli.command()
@click.option("--foo", default="foo")
def foo(foo):
    click.echo(foo)


@cli.command()
def bar():
    click.echo("bar")


r = CliRunner()


def test_default_command_with_arguments():
    assert r.invoke(cli, ["--foo", "foooo"]).output == "foooo\n"
    assert "No such option" in r.invoke(cli, ["-x"]).output


def test_group_arguments():
    assert r.invoke(cli, ["--group-only"]).output == "--group-only passed.\n"


def test_explicit_command():
    assert r.invoke(cli, ["foo"]).output == "foo\n"
    assert r.invoke(cli, ["bar"]).output == "bar\n"


def test_set_ignore_unknown_options_to_false():
    with pytest.raises(ValueError):
        DefaultGroup(ignore_unknown_options=False)


def test_default_if_no_args():
    cli = DefaultGroup()

    @cli.command()
    @click.argument("foo", required=False)
    @click.option("--bar")
    def foobar(foo, bar):
        click.echo(foo)
        click.echo(bar)

    cli.set_default_command(foobar)
    assert r.invoke(cli, []).output.startswith("Usage:")
    assert r.invoke(cli, ["foo"]).output == "foo\n\n"
    assert r.invoke(cli, ["foo", "--bar", "bar"]).output == "foo\nbar\n"
    cli.default_if_no_args = True
    assert r.invoke(cli, []).output == "\n\n"


def test_format_commands():
    help = r.invoke(cli, ["--help"]).output
    assert "foo*" in help
    assert "bar*" not in help
    assert "bar" in help


def test_deprecation():
    # @cli.command(default=True) has been deprecated since 1.2.
    cli = DefaultGroup()
    pytest.deprecated_call(cli.command, default=True)


if __name__ == "__main__":
    cli()
