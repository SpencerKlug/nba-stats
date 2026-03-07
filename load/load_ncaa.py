"""Backward-compat stub. Use: python -m load.ncaa"""

import sys

from load.ncaa import __main__ as ncaa_main

if __name__ == "__main__":
    sys.exit(ncaa_main.main())
