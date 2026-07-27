"""CLI entrypoint for listener-5.5."""

from __future__ import annotations

import argparse
import logging
import signal
import sys

from listener55 import __service_name__, __version__
from listener55.config import Config
from listener55.server import ListenerService


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="listener55",
        description="5.5 listener service with periodic self-reporting",
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
    )
    config = Config.from_env()
    config.validate()

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
        service.stop()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    try:
        service.start(blocking=True)
    except KeyboardInterrupt:
        service.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
