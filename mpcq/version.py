from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("mpcq")
except PackageNotFoundError:
    # package is not installed
    __version__ = "unknown"
