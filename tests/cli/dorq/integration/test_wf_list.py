from orquestra.sdk._base.cli._dorq._workflow._list import Action

class TestList:
    def test_all_parameters_passed(self, tmp_default_config_json):
        action = Action()
        action.on_cmd_call(config="proper_token", limit=None, max_age=None,
                           state=None, workspace_id="ws_id")
