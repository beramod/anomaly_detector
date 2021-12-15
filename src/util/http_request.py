import socket
import requests, json

class HttpRequests:
    emsApiServer = 'url'
    apiKey = 'key'
    headers = {'Content-Type': 'application/json'}

    def getIpAddress(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]

    def _ApiServerUrl(self):
        return self.emsApiServer

    def get(self, uri, parameters):
        parameter = ''

        for key in parameters:
            parameter = parameter + '&{}={}'.format(key, str(parameters.get(key)))
        try:
            response = requests.get('{}{}?aKey={}{}'.format(self._ApiServerUrl(), uri, self.apiKey, parameter), headers=self.headers)
            return response.json()
        except Exception as e:
            try:
                response = requests.get('{}{}?aKey={}{}'.format(self._ApiServerUrl(), uri, self.apiKey, parameter), headers=self.headers)
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
            response = requests.post('{}{}?aKey={}{}'.format(self._ApiServerUrl(), uri, self.apiKey, parameter), headers=self.headers,
                                    data=json.dumps(body))
            return response.json()
        except Exception as e:
            try:
                response = requests.post('{}{}?aKey={}{}'.format(self._ApiServerUrl(), uri, self.apiKey, parameter), headers=self.headers,
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
            response = requests.put('{}{}?aKey={}{}'.format(self._ApiServerUrl(), uri, self.apiKey, parameter), headers=self.headers,
                                    data=json.dumps(body))
            return response.json()
        except Exception as e:
            try:
                response = requests.put('{}{}?aKey={}{}'.format(self._ApiServerUrl(), uri, self.apiKey, parameter), headers=self.headers,
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
            response = requests.delete('{}{}?aKey={}{}'.format(self._ApiServerUrl(), uri, self.apiKey, parameter), headers=self.headers)
            return response.json()
        except Exception as e:
            try:
                response = requests.delete('{}{}?aKey={}{}'.format(self._ApiServerUrl(), uri, self.apiKey, parameter), headers=self.headers)
                return response.json()
            except Exception as e:
                return {
                    'code': 500,
                    'message': 'internal server error'
                }