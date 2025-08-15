"""
Base classes for AK Unified adapters.
"""


class BaseAdapterError(Exception):
    """Base exception class for all adapter errors."""
    
    def __init__(self, message: str, *args, **kwargs):
        super().__init__(message, *args, **kwargs)
        self.message = message
    
    def __str__(self):
        return self.message
    
    def __repr__(self):
        return f"{self.__class__.__name__}({self.message})"


class AdapterConnectionError(BaseAdapterError):
    """Raised when adapter cannot connect to data source."""
    pass


class AdapterAuthenticationError(BaseAdapterError):
    """Raised when adapter authentication fails."""
    pass


class AdapterRateLimitError(BaseAdapterError):
    """Raised when adapter hits rate limits."""
    pass


class AdapterDataError(BaseAdapterError):
    """Raised when adapter receives invalid or unexpected data."""
    pass


class AdapterTimeoutError(BaseAdapterError):
    """Raised when adapter operation times out."""
    pass


class AdapterNotSupportedError(BaseAdapterError):
    """Raised when requested operation is not supported by adapter."""
    pass