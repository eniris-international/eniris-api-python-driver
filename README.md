# Eniris API driver for Python
This repository contains the official Eniris API driver for Python. This driver takes care of authentication as well as request retries and timeouts, in accordance with the [authentication API documentation](https://authentication.eniris.be/docs). It offers users an interface which is inspired by the popular [requests](https://requests.readthedocs.io/en/latest/) library.

## Installation
To install the latest stable version, use:
```sh
pip install eniris
```
## Quick Example
```python
from eniris import ApiDriver

driver = ApiDriver("myUsername", "myPassword")
http_response = driver.get("/v1/device")
response_body = http_response.json()
print(response_body['device'][:10])
driver.close()
```
## Details
The driver constructor accepts the following arguments:
- username (string, required)
- password (string, required)
- authUrl (string, optional, default: 'https://authentication.eniris.be'): URL of authentication endpoint
- apiUrl (string, optional, default: 'https://api.eniris.be'): URL of api endpoint
- timeoutS (int, optional, default: 60): Request timeout in seconds
- maximumRetries (int, optional, default: 5): How many times to try again in case of a failure due to connection or unavailability problems
- initialRetryDelayS (int, optional, default: 1): The initial delay between successive retries in seconds.
- maximumRetryDelayS (int, optional, default: 60): The maximum delay between successive retries in seconds.
- session (requests.Session, optional, default: requests.Session()): A session object to use for all API calls.

Furthermore, the following methods are exposed:
- accesstoken: Get a currently valid accesstoken
- get/delete: Send a HTTP GET/DELETE request. The following parameters are allowed:
  - path (string, required): Either a path relative to the apiUrl, or a full URL path
  - params (dict, optional, default: None): URL query parameters
- post/put: Send a HTTP POST or PUT request. The following parameters are allowed:
  - path (string, required): Either a path relative to the apiUrl, or a full URL path
  - json (dict, optional, default: None): JSON body of the request. The json argument and the data argument cannot both be different from None
  - params (dict, optional, default: None): URL query parameters
  - data (string or dict, optional, default: None): Payload of the request. The json argument and the data argument cannot both be different from None