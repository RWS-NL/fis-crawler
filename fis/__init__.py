try:
    from ._version import version as __version__
except ImportError:
    from importlib.metadata import version, PackageNotFoundError
    try:
        __version__ = version("fis")
    except PackageNotFoundError:
        __version__ = "unknown"
