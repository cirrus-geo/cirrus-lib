class InvalidInput(Exception):
    """Exception class for when processing fails due to invalid input

    Args:
        Exception (Exception): Base class
    """
    pass


class NoUrlError(ValueError):
    """Exception class for when a payload does not have a URL."""
    pass
