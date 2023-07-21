import pytest
from _pytest.capture import CaptureManager

from orquestra.sdk._base.cli._dorq._workflow._list import Action
from readchar import key

# Those tests are testing entirety of CLI system interactions with our API.
# Runtime is mocked
class TestList:
    def test_all_parameters_passed(self, capsys, tmp_default_config_json, fake_list_runtime):
        action = Action()

        action.on_cmd_call(config="proper_token", limit=None, max_age=None,
                           state=None, workspace_id="ws_id")

        stdout = capsys.readouterr().out

        # Should receive 4 running workflows
        assert stdout.count("RUNNING") == 4


    def test_no_parameters_passed(self, monkeypatch, pytestconfig, tmp_default_config_json, fake_list_runtime):
        capmanager = pytestconfig.pluginmanager.getplugin('capturemanager')
        capmanager.suspend_global_capture(in_=True)

        gen = (k for k in (key.DOWN, key.ENTER))

        def key_stroker(*_):
            return next(gen)

        monkeypatch.setattr('sys.stdin.read', key_stroker)
        action = Action()
        action.on_cmd_call(config=None, limit=None, max_age=None,
                           state=None, workspace_id=None)
