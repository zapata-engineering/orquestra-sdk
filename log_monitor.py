import logging
from pathlib import Path
import time
import watchfiles
import watchdog
import watchdog.events
import watchdog.observers
from pprint import pprint


def main1():
    for changes in watchfiles.watch("."):
        pprint(list(changes))


def main2():
    logging.basicConfig(level=logging.INFO)
    handler = watchdog.events.LoggingEventHandler()
    observer = watchdog.observers.Observer()
    log_path = Path("~/.orquestra/ray/session_latest/logs").expanduser()
    print(f"Monitoring {log_path}")
    observer.schedule(handler, log_path, recursive=True)
    observer.start()

    while True:
        time.sleep(100)


if __name__ == "__main__":
    main2()
