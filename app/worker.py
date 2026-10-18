"""Worker process scaffold; Redis Streams processing is the next implementation step."""
import logging
import os
import signal
import time

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)
stopping = False


def stop(signum: int, frame: object) -> None:
    global stopping
    stopping = True
    logger.info("worker_shutdown_requested signal=%s", signum)


def main() -> None:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    logger.info("worker_started concurrency=%s", os.getenv("WORKER_CONCURRENCY", "4"))
    while not stopping:
        time.sleep(1)
    logger.info("worker_stopped")


if __name__ == "__main__":
    main()
