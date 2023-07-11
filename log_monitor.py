import logging
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
    observer.schedule(handler, ".", recursive=True)
    observer.start()

    while True:
        time.sleep(1)


if __name__ == "__main__":
    main2()
