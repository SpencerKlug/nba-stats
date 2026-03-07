"""NCAA men's basketball data ingestion from stats.ncaa.org."""

from load.ncaa import client, fetch

__all__ = ["client", "fetch"]
