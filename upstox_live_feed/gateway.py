"""Run the memory-bounded local market-data gateway."""

from __future__ import annotations

import logging

import uvicorn

from .api import create_app
from .gateway_config import GatewaySettings


def main() -> None:
    settings = GatewaySettings.from_environment()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    uvicorn.run(
        create_app(settings),
        host=settings.host,
        port=settings.port,
        workers=1,
        access_log=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()
