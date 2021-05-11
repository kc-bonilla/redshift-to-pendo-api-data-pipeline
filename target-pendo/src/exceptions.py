class PendoError(Exception):
    def __init__(self, message=None, context=None):
        self.message = message
        self.context = context


class PendoClientResponseError(PendoError, Exception):
    def __init__(self, status, response_body):
        Super().__init__()
        self.status = status
        self.response = response_body


class TargetPendoException(Exception):
    """A known exception for which we don't need to print a stack trace"""


class ArgumentError(ValueError, PendoError):
    pass


class HttpError(PendoError):
    pass


class ResourceNotFound(PendoError):
    pass


class AuthenticationError(PendoError):
    pass


class ServerError(PendoError):
    pass


class BadGatewayError(PendoError):
    pass


class ServiceUnavailableError(PendoError):
    pass


class BadRequestError(PendoError):
    pass


class RateLimitExceeded(PendoError):
    pass


class ResourceNotRestorable(PendoError):
    pass


class MultipleMatchingUsersError(PendoError):
    pass


class UnexpectedError(PendoError):
    pass


class TokenUnauthorizedError(PendoError):
    pass


class TokenNotFoundError(PendoError):
    pass


error_codes = {
    'unauthorized': AuthenticationError,
    'forbidden': AuthenticationError,
    'bad_request': BadRequestError,
    'action_forbidden': BadRequestError,
    'missing_parameter': BadRequestError,
    'parameter_invalid': BadRequestError,
    'parameter_not_found': BadRequestError,
    'client_error': BadRequestError,
    'type_mismatch': BadRequestError,
    'not_found': ResourceNotFound,
    'admin_not_found': ResourceNotFound,
    'not_restorable': ResourceNotRestorable,
    'rate_limit_exceeded': RateLimitExceeded,
    'service_unavailable': ServiceUnavailableError,
    'server_error': ServiceUnavailableError,
    'conflict': MultipleMatchingUsersError,
    'unique_user_constraint': MultipleMatchingUsersError,
    'token_unauthorized': TokenUnauthorizedError,
    'token_not_found': TokenNotFoundError,
    'token_revoked': TokenNotFoundError,
    'token_blocked': TokenNotFoundError,
    'token_expired': TokenNotFoundError
}


class HTTPError(Exception):
    """Base class for 'RequestError' and 'HTTPStatusError'"""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class RequestError(HTTPError):
    """Base class for all exceptions that may
    occur when issuing a '.request()'
    """

    def __init__(self, message: str, *, request: "Request" = None) -> None:
        super().__init__(message)
        # At the point an exception is raised we won't typically have a request
        # instance to associate it with.
        self._request = request


class TransportError(RequestError):
    """Base class for all exceptions that occur
    at the level of the Transport API
    """


class NetworkError(TransportError):
    """The base class for network-related errors.
    An error occurred while interacting with the network.
    """


class WriteError(NetworkError):
    """Failed to send data through the network"""
