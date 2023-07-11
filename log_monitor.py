import watchfiles
from pprint import pprint


def main():
    for changes in watchfiles.watch("."):
        pprint(list(changes))


if __name__ == "__main__":
    main()
