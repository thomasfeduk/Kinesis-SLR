class ConfigValidationError(Exception):
    """Raised when a configuration parameter is invalid."""
    pass


class FileProcessingError(Exception):
    """Raised when a configuration parameter is invalid."""
    pass


class InternalError(Exception):
    """Raised when an internal exception occurs. Could be connections, logic issues, conditions etc."""
    pass


class InvalidArgumentException(InternalError):
    """Raised when an invalid argument is passed to a method. If it got this far, validation was missed"""
    pass


class InternalConnectionError(InternalError):
    """Raised when an internal system to another internal system failed to connect"""
    pass


class UnexpectedInternalHostResponse(InternalError):
    """Raised when data from an internal system in not in expected form/value"""
    pass


class AwsError(Exception):
    """Sub-base exception for errors related to aws (unexpected response, permission denied etc)"""
    pass


class AwsErrorLambdaInvocationFailed(Exception):
    """Sub-base exception for errors related to aws (unexpected response, permission denied etc)"""
    pass


class AwsConnectionError(AwsError):
    """Raised when data from an AWS service system could not connect"""
    pass


class AwsUnexpectedResponse(AwsError):
    """When an unexpected response comes from an aws resource (permission denied, record not found etc)"""
    pass
