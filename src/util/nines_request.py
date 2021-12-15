import socket
import requests, json


class NinesRequests:
    ninesApiServer = 'url'
    ninesKey = 'key'
    headers = {'Content-Type': 'application/json'}

    def _ApiServerUrl(self):
        return self.ninesApiServer

    def get(self, uri, parameters):
        parameter = ''

        for key in parameters:
            parameter = parameter + '&{}={}'.format(key, str(parameters.get(key)))
        try:
            response = requests.get('{}{}?aKey={}{}'.format(self._ApiServerUrl(), uri, self.ninesKey, parameter), headers=self.headers)
            return response.json()
        except Exception as e:
            try:
                response = requests.get('{}{}?aKey={}{}'.format(self._ApiServerUrl(), uri, self.ninesKey, parameter), headers=self.headers)
                return response.json()
            except Exception as e:
                return {
                    'code': 500,
                    'message': 'internal server error'
                }

    def post(self, uri, parameters, body):
        parameter = ''

        for key in parameters:
            parameter = parameter + '&{}={}'.format(key, str(parameters.get(key)))
        try:
            response = requests.post('{}{}?aKey={}{}'.format(self._ApiServerUrl(), uri, self.ninesKey, parameter), headers=self.headers,
                                    data=json.dumps(body))
            return response.json()
        except Exception as e:
            try:
                response = requests.post('{}{}?aKey={}{}'.format(self._ApiServerUrl(), uri, self.ninesKey, parameter), headers=self.headers,
                                         data=json.dumps(body))
                return response.json()
            except Exception as e:
                return {
                    'code': 500,
                    'message': 'internal server error'
                }

    def put(self, uri, parameters, body):
        parameter = ''
        for key in parameters:
            parameter = parameter + '&{}={}'.format(key, str(parameters.get(key)))
        try:
            response = requests.put('{}{}?aKey={}{}'.format(self._ApiServerUrl(), uri, self.ninesKey, parameter), headers=self.headers,
                                    data=json.dumps(body))
            return response.json()
        except Exception as e:
            try:
                response = requests.put('{}{}?aKey={}{}'.format(self._ApiServerUrl(), uri, self.ninesKey, parameter), headers=self.headers,
                                        data=json.dumps(body))
                return response.json()
            except Exception as e:
                return {
                    'code': 500,
                    'message': 'internal server error'
                }

    def delete(self, uri, parameters):
        parameter = ''

        for key in parameters:
            parameter = parameter + '&{}={}'.format(key, str(parameters.get(key)))
        try:
            response = requests.delete('{}{}?aKey={}{}'.format(self._ApiServerUrl(), uri, self.ninesKey, parameter), headers=self.headers)
            return response.json()
        except Exception as e:
            try:
                response = requests.delete('{}{}?aKey={}{}'.format(self._ApiServerUrl(), uri, self.ninesKey, parameter), headers=self.headers)
                return response.json()
            except Exception as e:
                return {
                    'code': 500,
                    'message': 'internal server error'
                }