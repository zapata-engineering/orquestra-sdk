import asyncio
from unittest.mock import ANY, create_autospec

import aiohttp
import pytest
from aiohttp import web

from orquestra.sdk._base.cli._dorq._login._login_server import LoginServer
from orquestra.sdk._base.cli._dorq._repos import RuntimeRepo
from orquestra.sdk._base.cli._dorq._ui._presenters import LoginPresenter


@pytest.mark.parametrize("is_ce", [True, False])
@pytest.mark.parametrize("timeout", [0, 60])
class TestLoginServer:
    def test_happy_path(
        self, is_ce: bool, timeout: int, monkeypatch: pytest.MonkeyPatch
    ):
        cluster_url = "cluster_url"
        listen_host = "127.0.0.1"
        login_url = "login__url"

        runtime_repo = create_autospec(RuntimeRepo)
        presenter = create_autospec(LoginPresenter)
        sleep = create_autospec(asyncio.sleep)
        runtime_repo.get_login_url.return_value = login_url
        presenter.open_url_in_browser.return_value = True
        monkeypatch.setattr(asyncio, "sleep", sleep)
        server = LoginServer(
            runtime_repo,
            presenter,
        )

        asyncio.run(server.run(cluster_url, is_ce, listen_host, timeout))

        runtime_repo.get_login_url.assert_called_once_with(cluster_url, is_ce, ANY)
        presenter.open_url_in_browser.assert_called_once_with(login_url)
        presenter.print_login_help.assert_called_once()
        sleep.assert_called_once_with(timeout)
        assert server.login_url == login_url

    def test_failed_to_open_browser(
        self, is_ce: bool, timeout: int, monkeypatch: pytest.MonkeyPatch
    ):
        cluster_url = "cluster_url"
        listen_host = "127.0.0.1"
        login_url = "login__url"

        runtime_repo = create_autospec(RuntimeRepo)
        presenter = create_autospec(LoginPresenter)
        sleep = create_autospec(asyncio.sleep)
        runtime_repo.get_login_url.return_value = login_url
        presenter.open_url_in_browser.return_value = False
        monkeypatch.setattr(asyncio, "sleep", sleep)
        server = LoginServer(
            runtime_repo,
            presenter,
        )

        asyncio.run(server.run(cluster_url, is_ce, listen_host, timeout))

        runtime_repo.get_login_url.assert_called_once_with(cluster_url, is_ce, ANY)
        presenter.open_url_in_browser.assert_called_once_with(login_url)
        presenter.print_login_help.assert_not_called()
        sleep.assert_not_called()
        assert server.login_url == login_url


async def make_request(app: web.Application, url: str) -> aiohttp.ClientResponse:
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "localhost", 8080)
    try:
        await site.start()

        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://localhost:8080{url}") as response:
                pass
    finally:
        await site.stop()
    return response


class TestHandlers:
    def test_no_token(self):
        cluster_url = "cluster_url"
        runtime_repo = create_autospec(RuntimeRepo)
        presenter = create_autospec(LoginPresenter)
        server = LoginServer(
            runtime_repo,
            presenter,
        )

        app = web.Application()
        app["cluster_url"] = cluster_url
        app.router.add_get("/state", server._state_handler)

        resp = asyncio.run(make_request(app, "/state"))

        assert resp.status == 400
        assert "Access-Control-Allow-Origin" in resp.headers
        assert resp.headers["Access-Control-Allow-Origin"] == cluster_url

    def test_with_token(self):
        cluster_url = "cluster_url"
        token = "test"
        runtime_repo = create_autospec(RuntimeRepo)
        presenter = create_autospec(LoginPresenter)
        server = LoginServer(
            runtime_repo,
            presenter,
        )

        app = web.Application()
        app["cluster_url"] = cluster_url
        app.router.add_get("/state", server._state_handler)

        with pytest.raises(web.GracefulExit):
            _ = asyncio.run(make_request(app, f"/state?token={token}"))

        assert server.token == token
