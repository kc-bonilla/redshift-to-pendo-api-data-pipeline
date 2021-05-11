# -*- coding: utf-8 -*-
import requests


class Client(object):

    def __init__(self, integration_key='X-Pendo-Integration-Key'):
        self.integration_key = integration_key
        self.base_url = 'https://app.pendo.io'
        self.rate_limit_details = {}
        self.http_session = requests.Session()

    @property
    def _auth(self):
        return self.integration_key, ''

    def _execute_request(self, request, params):
        result, response = request.execute(self.base_url, self._auth, params)
        return result, response

    def get(self, path, params):
        from target_pendo import request
        req = request.Request('GET', path, self.http_session)
        return self._execute_request(req, params)

    def post(self, path, params):
        from target_pendo import request
        req = request.Request('POST', path, self.http_session)
        return self._execute_request(req, params)

    def put(self, path, params):
        from target_pendo import request
        req = request.Request('PUT', path, self.http_session)
        return self._execute_request(req, params)

    def delete(self, path, params):
        from target_pendo import request
        req = request.Request('DELETE', path, self.http_session)
        return self._execute_request(req, params)
