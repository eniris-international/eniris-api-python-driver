# Eniris API driver for Python
This repository contains the official Eniris API driver for Python. This driver takes care of authentication as well as request retries and timeouts, in accordance with the [authentication API documentation](https://authentication.eniris.be/docs). It offers users an interface which is inspired by the popular [requests](https://requests.readthedocs.io/en/latest/) library.

## Installation
To install the latest stable version, use:
```sh
pip install neo4j
```
## Quick Example
```python
from eniris import ApiDriver

driver = ApiDriver("myUsername", "myPassword")
http_response = driver.get("/v1/device")
print(http_response['device'][:10])
driver.close()
```
## Details
The driver constructor accepts the following arguments:
- username (string, required)
- password (string, required)
- authUrl (string, optional, default: 'https://authentication.eniris.be'): URL of authentication endpoint
- apiUrl (string, optional, default: 'https://api.eniris.be'): URL of api endpoint
- maxRetries (int, optional, default: 5): How many times to try again in case of a failure due to connection or unavailability problems
- timeoutS (int, optional, default: 60): Request timeout in seconds

Furthermore, the following methods are exposed:
- get/delete: Send a HTTP GET/DELETE request. The following parameters are allowed:
  - path (string, required): Either a path relative to the apiUrl, or a full URL path
  - params (dict, optional, default: None): URL query parameters
- post/put: Send a HTTP POST or PUT request. The following parameters are allowed:
  - path (string, required): Either a path relative to the apiUrl, or a full URL path
  - json (dict, optional, default: None): JSON body of the request. The json argument and the data argument cannot both be different from None
  - params (dict, optional, default: None): URL query parameters
  - data (string or dict, optional, default: None): Payload of the request. The json argument and the data argument cannot both be different from None