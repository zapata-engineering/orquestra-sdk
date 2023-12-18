################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

from typing import Optional

from aiohttp import web

CLUSTER_URL_KEY = web.AppKey("cluster_url", str)


class LoginServer:
    def __init__(self):
        self._token: Optional[str] = None
        self._login_url: Optional[str] = None

    @property
    def token(self):
        return self._token

    async def start(self, cluster_url: str, listen_host: str = "127.0.0.1") -> int:
        app = web.Application()
        app[CLUSTER_URL_KEY] = cluster_url
        app.add_routes([web.get("/state", self._state_handler)])
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, listen_host, 0)
        await site.start()
        return self._runner.addresses[0][1]

    async def cleanup(self):
        await self._runner.cleanup()

    async def _send_response(self, request: web.Request, status_code: int):
        """Sends an empty response with a specified status code with CORS."""
        res = web.StreamResponse(
            status=status_code,
            headers={
                "Access-Control-Allow-Origin": request.app[CLUSTER_URL_KEY],
                "Content-Length": "0",
            },
        )
        await res.prepare(request)

    async def _state_handler(self, request: web.Request):
        token = request.query.get("token")
        if token is not None:
            self._token = token
            await self._send_response(request, 200)
            raise web.GracefulExit
        await self._send_response(request, 400)
        return web.Response()
