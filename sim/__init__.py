"""Simulation package: a deterministic middleware estate plus fault injection."""

from .estate import Estate
from .incidents import SCENARIOS, inject

__all__ = ["Estate", "inject", "SCENARIOS"]
