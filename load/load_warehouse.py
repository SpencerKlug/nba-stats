"""Backward-compat stub. Use: python -m load.nba"""

import sys

from load.nba import __main__ as nba_main

if __name__ == "__main__":
    sys.exit(nba_main.main())
