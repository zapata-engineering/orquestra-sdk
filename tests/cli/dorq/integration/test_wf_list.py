from orquestra.sdk._base.cli._dorq._workflow._list import Action
import orquestra.sdk._base._api._config
import os


class TestList:
    def test_all_parameters_passed(self, capsys, tmp_default_config_json, mock_runtime):
        action = Action()

        action.on_cmd_call(config=["proper_token"], limit=None, max_age=None,
                           state=None, workspace_id="ws_id", project_id="x")

        stdout = capsys.readouterr().out

        # Should receive 4 running workflows
        assert stdout.count("RUNNING") == 4
