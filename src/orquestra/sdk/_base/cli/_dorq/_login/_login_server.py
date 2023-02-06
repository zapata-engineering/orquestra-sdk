import asyncio
from typing import Optional

from aiohttp import web


class LoginServer:
    def __init__(
        self,
        runtime_repo,
        presenter,
    ):
        self._runtime_repo = runtime_repo
        self._presenter = presenter
        self._token: Optional[str] = None
        self._login_url: Optional[str] = None

    @property
    def token(self):
        return self._token

    @property
    def login_url(self):
        return self._login_url

    async def run(
        self,
        cluster_url: str,
        is_ce: bool,
        listen_host: str,
        timeout: int,
    ):
        app = web.Application()
        app["cluster_url"] = cluster_url
        app.add_routes([web.get("/state", self._state_handler)])
        runner = web.AppRunner(app)
        await runner.setup()
        try:
            site = web.TCPSite(runner, listen_host, 0)
            await site.start()
            self._login_url = self._runtime_repo.get_login_url(
                cluster_url, is_ce, runner.addresses[0][1]
            )
            browser_opened = self._presenter.open_url_in_browser(self._login_url)
            # If we managed to open a browser, keep the server open
            if browser_opened:
                self._presenter.print_login_help()
                await asyncio.sleep(timeout)
        finally:
            await runner.cleanup()

    async def _send_response(self, request: web.Request, status_code: int):
        """Sends an empty response with a specified status code with CORS"""
        res = web.StreamResponse(
            status=status_code,
            headers={
                "Access-Control-Allow-Origin": request.app["cluster_url"],
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
