"""xoto3"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("xoto3")
except PackageNotFoundError:
    __version__ = "unknown"

__author__ = "Peter Gaultney"
__author_email__ = "pgaultney@xoi.io"
