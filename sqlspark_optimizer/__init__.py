"""sqlspark_optimizer — multi-agent SQL->PySpark query optimizer.

Public API:
    from sqlspark_optimizer import optimize, OptimizeResult
"""
from sqlspark_optimizer.api import OptimizeResult, optimize

__version__ = "0.1.0"
__all__ = ["optimize", "OptimizeResult", "__version__"]
