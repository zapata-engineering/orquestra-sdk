from dataclasses import dataclass
import typing as t
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


@dataclass(frozen=True)
class FileKeepsake:
    """
    Keeps data about the last time we accessed a log file.
    """

    modified_at: int
    """
    Last seen modification time. In nanoseconds.
    """

    last_position: int
    """
    Result of last .tell().
    """


class FileMonitor:
    def __init__(self, paths: t.Sequence[Path]):
        self._paths = paths
        self._keepsakes: t.Dict[Path, FileKeepsake] = {}

    def find_changes(self) -> t.Generator[t.Tuple[Path, str], None, None]:
        """
        Runs one round of polling for newly added lines.

        Relies on "modified at" file metadata. Overrides `self._keepsakes`.
        """
        new_keepsakes = {}
        for file_path in self._paths:
            stat = file_path.stat()
            modified_at = stat.st_mtime_ns
            # 2. Check if updated since last keepsake, or read full and create a new
            # keepsake.
            if (last_keepsake := self._keepsakes.get(file_path)) is not None:
                if modified_at > last_keepsake.modified_at:
                    # 3. Advance pointer and read the new content
                    with file_path.open() as f:
                        f.seek(last_keepsake.last_position)
                        new_content = f.read()
                        new_position = f.tell()

                    yield file_path, new_content

                    new_keepsakes[file_path] = FileKeepsake(
                        modified_at=modified_at,
                        last_position=new_position,
                    )
                else:
                    # File unmodified.
                    new_keepsakes[file_path] = last_keepsake
            else:
                with file_path.open() as f:
                    new_content = f.read()
                    new_position = f.tell()

                yield file_path, new_content

                new_keepsakes[file_path] = FileKeepsake(
                    modified_at=modified_at,
                    last_position=new_position,
                )

        self._keepsakes = new_keepsakes


def main3():
    log_path = Path("~/.orquestra/ray/session_latest/logs").expanduser()
    print(f"Monitoring {log_path}")

    # 1. Find matching worker files
    worker_paths = list(log_path.glob("worker-*-*-*.???"))
    print(worker_paths)

    monitor = FileMonitor(worker_paths)

    round_counter = 0
    while True:
        print(f"Round {round_counter}")
        for worker_path, new_content in monitor.find_changes():
            print(f"Detected change in {worker_path}")
            print("New content:")
            print(new_content)
            print("-" * 20)

        print("\n" * 3)

        time.sleep(4)
        round_counter += 1


if __name__ == "__main__":
    main3()
