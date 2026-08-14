"""Retired Linux controller route.

The released public controller is Windows-native.  WSL is admitted only for
the controller-owned provider bridge, so this historical entry point refuses
to launch anything and cannot accidentally recreate the old Linux route.
"""

from __future__ import annotations

import argparse


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Retired: public controller is host-native"
    )
    parser.parse_args()
    raise RuntimeError(
        "public controller route is Windows-native; WSL may launch only the provider bridge"
    )


if __name__ == "__main__":
    raise SystemExit(main())
