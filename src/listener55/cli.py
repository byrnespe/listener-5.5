"""CLI entrypoint for listener-5.5."""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import os
import signal
import sys

from listener55 import __service_name__, __version__
from listener55.config import Config
from listener55.server import ListenerService


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="listener55",
        description=(
            "5.5 listener: self-healing fleet health bus with an agent interface. "
            "With no command it runs the service."
        ),
    )
    p.add_argument(
        "command",
        nargs="?",
        default="serve",
        choices=["serve", "mcp", "tools"],
        help=(
            "serve: run the listener (default). "
            "mcp: speak MCP on stdio, relayed to a running listener. "
            "tools: print the tool catalog for an agent framework."
        ),
    )
    p.add_argument(
        "--url",
        default=None,
        help="mcp: base URL of the running listener (default $LISTENER55_URL or http://127.0.0.1:$LISTENER55_PORT)",
    )
    p.add_argument(
        "--format",
        default="neutral",
        choices=["neutral", "mcp", "openai", "anthropic", "gemini"],
        help="tools: which function-calling dialect to print (default neutral)",
    )
    p.add_argument("--version", action="version", version=f"{__service_name__} {__version__}")
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default INFO)",
    )
    p.add_argument(
        "--once-report",
        action="store_true",
        help="Emit a single self-report using current env config and exit",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,  # stdout belongs to the protocol in `mcp` mode
    )
    config = Config.from_env()
    config.validate()

    if args.command == "mcp":
        from listener55.agent import run_stdio_bridge

        host = "127.0.0.1" if config.host in ("0.0.0.0", "::") else config.host
        url = args.url or os.environ.get("LISTENER55_URL") or f"http://{host}:{config.port}"
        return run_stdio_bridge(url, config.agent_token)

    if args.command == "tools":
        # Built against throwaway in-memory state: printing a catalog must not
        # touch, or depend on, the running service's files.
        state = ListenerService(dataclasses.replace(config, state_dir=None)).state
        print(json.dumps(state.agent.catalog(args.format), indent=2))
        return 0

    if args.once_report:
        from listener55.reporter import SelfReporter
        from listener55.schema import Metrics

        metrics = Metrics()
        metrics.set_status("healthy")
        reporter = SelfReporter(
            metrics=metrics,
            instance_id=config.instance_id,
            host=config.host,
            port=config.port,
            interval_seconds=config.report_interval_seconds,
            report_url=config.report_url,
            report_log_path=config.report_log_path,
        )
        payload = reporter.emit_once()
        print(payload)
        return 0

    service = ListenerService(config)

    def _stop(signum: int, _frame: object) -> None:
        logging.getLogger("listener55").info("signal %s — shutting down", signum)
        service.request_stop()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    try:
        service.start(blocking=True)
    except KeyboardInterrupt:
        service.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
