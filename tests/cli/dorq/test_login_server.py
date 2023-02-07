import asyncio
import sys

import pytest
from aiohttp import ClientResponse, ClientSession, web

from orquestra.sdk._base.cli._dorq._login._login_server import LoginServer


class TestLoginServer:
    @pytest.mark.skipif(
        sys.platform.startswith("win"), reason="This test is buggy on windows"
    )
    def test_happy_path(self):
        cluster_url = "cluster_url"
        server = LoginServer()

        port = asyncio.run(server.start(cluster_url))

        assert isinstance(port, int)
        assert port > 0

        asyncio.run(server.cleanup())


async def make_request(app: web.Application, url: str) -> ClientResponse:
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "localhost", 8080)
    try:
        await site.start()

        async with ClientSession() as session:
            async with session.get(f"http://localhost:8080{url}") as response:
                pass
    finally:
        await site.stop()
    return response


class TestHandlers:
    def test_no_token(self):
        cluster_url = "cluster_url"
        server = LoginServer()

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
        server = LoginServer()

        app = web.Application()
        app["cluster_url"] = cluster_url
        app.router.add_get("/state", server._state_handler)

        with pytest.raises(web.GracefulExit):
            _ = asyncio.run(make_request(app, f"/state?token={token}"))

        assert server.token == token
