import os
import json
import requests
from singer.logger import get_logger
from datetime import datetime
from ratelimit import limits, RateLimitException
from backoff import on_exception, expo
from target_pendo import errors

LOGGER = get_logger()


def configure_timeout():
    """Configure the request timeout"""
    timeout = os.getenv('PENDO_REQUEST_TIMEOUT', '300')
    try:
        return int(timeout)
    except ValueError:
        LOGGER.warning(f"{timeout} IS NOT A VALID TIMOUT VALUE")
    return 300


class Request(object):
    timeout = configure_timeout()

    def __init__(self, http_method, path, http_session=None):
        self.http_method = http_method
        self.http_session = http_session
        self.path = path

    def execute(self, base_url, auth, params):
        return self.send_request_to_path(base_url, auth, params)
        
    @on_exception(expo, RateLimitException, max_tries=3)
    @limits(calls=30000, period=300)
    def send_request_to_path(self, base_url, auth, params):
        """Constructs a request, sends to the Client, parses response"""
        req_params = {}
        url = base_url + self.path
        headers = {
            'User-Agent': 'Singer-ShootProof',
            'Accept-Encoding': 'gzip, deflate',
            'Accept': 'application/json',
            'X-Pendo-Integration-Key': auth[0]
        }
        if self.http_method in ('POST', 'PUT', 'DELETE'):
            headers['content-type'] = 'application/json'
            req_params['json'] = params
        elif self.http_method == 'GET':
            req_params['params'] = params
        req_params['headers'] = headers
        LOGGER.info(f"HEADERS: {headers}")
        if self.http_session:
            resp = requests.request(
                self.http_method, url=url, timeout=self.timeout, auth=auth, **req_params
            )
        else:
            resp = self.http_session.request(
                self.http_method, url=url, timeout=self.timeout, auth=auth, **req_params
            )
        
        parsed_body = self.parse_body(resp)
        self.raise_errors_on_failure(resp)
        return parsed_body, resp.status_code

    def parse_body(self, resp):
        if resp.content and resp.content.strip():
            try:
                decoded_body = resp.content.decode(resp.encoding or resp.apparent_encoding)
                body = json.loads(decoded_body)
                if body.get('type') == 'error.list':
                    self.raise_application_errors_on_failure(body, resp.status_code)
                return body
            except ValueError:
                self.raise_errors_on_failure(resp)

    @staticmethod
    def raise_errors_on_failure(resp):
        if resp.status_code == 404:
            raise errors.ResourceNotFound('Resource Not Found')
        elif resp.status_code == 401:
            raise errors.AuthenticationError('Unauthorized')
        elif resp.status_code == 403:
            raise errors.AuthenticationError('Forbidden')
        elif resp.status_code == 500:
            raise errors.ServerError('Server Error')
        elif resp.status_code == 502:
            raise errors.BadGatewayError('Bad Gateway Error')
        elif resp.status_code == 503:
            raise errors.ServiceUnavailableError('Service Unavailable')

    def raise_application_errors_on_failure(self, error_list_details, http_code):
        error_details = error_list_details['errors'][0]
        error_code = error_details.get('type')
        error_code = error_details.get('code') if error_code else error_code
        error_context = {
            'http_code': http_code,
            'application_error_code': error_code
        }
        error_class = errors.error_codes.get(error_code)
        if error_class is None:
            if error_code:
                message = self.message_for_unexpected_error_with_type(error_details, http_code)
            else:
                message = self.message_for_unexpected_error_without_type(error_details, http_code)
            error_class = errors.UnexpectedError
        else:
            message = error_details.get('message')
        raise error_class(message, error_context)

    @staticmethod
    def message_for_unexpected_error_with_type(error_details, http_code):
        error_type = error_details.get('type')
        message = error_details.get('message')
        return (f"The error of type {error_type} is not recognized.\n" +
                f"It occurred with the message: {message} and HTTP code: {http_code}\n" +
                "Please contact Pendo with these details")

    @staticmethod
    def message_for_unexpected_error_without_type(error_details, http_code):
        message = error_details['message']
        return (f"An unexpected error occurred with the message: {message} and HTTP code: {http_code}\n" +
                "Please contact Pendo with these details")


class ResourceEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'attributes'):
            return obj.attributes
        return super(ResourceEncoder, self).default(obj)
