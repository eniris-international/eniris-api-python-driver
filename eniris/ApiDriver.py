import datetime
import requests
import logging

class AuthenticationFailure(Exception):
    "Raised when failing to authentiate to the Insights API"
    pass
    
class RetryFailure(Exception):
    "Raised when authentication was succesful, an API call failed repeatedly"
    pass

# Provide an easy interface to interact with the API, in the style of the requests library (https://docs.python-requests.org/en/master/)
class ApiDriver:
  def __init__(self, username:str, password:str, authUrl:str = 'https://authentication.eniris.be', apiUrl:str = 'https://api.eniris.be',  maxRetries:int = 5, timeoutS:int = 60):
    """Constructor. You must specify at least username and password or a credentialsPath where they are stored as a json of the form {'login': USERNAME, 'password': PASSWORD}

    Args:
        username (str, optional): Insights username
        password (str, optional): Insights password of the user
        authUrl (str, optional): Url of authentication endpoint. Defaults to https://authentication.eniris.be
        apiUrl (str, optional): Url of api endpoint. Defaults to https://api.eniris.be
        maxRetries (int, optional): How many times to try again in case of a failure. Defaults to 5.
        timeout (int, optional): API timeout in seconds. Defaults to 60.

    Raises:
        Exception: _description_
    """
    self.username = username
    self.password = password
    self.authUrl = authUrl
    self.apiUrl = apiUrl
    self.maxRetries = maxRetries
    self.timeoutS = timeoutS
    self.refreshDtAndToken = None
    self.accessDtAndToken = None
    
  def _authenticate(self):
    dt = datetime.datetime.now()
    if self.refreshDtAndToken is None or (dt - self.refreshDtAndToken[0]).total_seconds() > 13*24*60*60: # 13 days
      data = { "username": self.username, "password": self.password }
      resp = requests.post(self.authUrl + '/auth/login', json = data, timeout=self.timeoutS)
      if resp.status_code != 200:
        raise AuthenticationFailure("login failed: " + resp.text)
      self.refreshDtAndToken = (dt, resp.text)
    elif (dt - self.refreshDtAndToken[0]).total_seconds() > 7*24*60*60: # 7 days
      resp = requests.get(self.authUrl + '/auth/refreshtoken', headers = {'Authorization': 'Bearer ' + self.refreshDtAndToken[1]}, timeout=self.timeoutS)
      if resp.status_code == 200:
        self.refreshDtAndToken = (dt, resp.text)
      else:
        # Not the biggest problem, sice the refresh token will still be valid for a while, but we should log an exception
        logging.warning("Unable to renew the refresh token: " + resp.text)
    if self.accessDtAndToken is None or (dt - self.accessDtAndToken[0]).total_seconds() > 2*60: # 2 minutes
      resp = requests.get(self.authUrl + '/auth/accesstoken', headers = {'Authorization': 'Bearer ' + self.refreshDtAndToken[1]}, timeout=self.timeoutS)
      if resp.status_code != 200:
        raise AuthenticationFailure("accesstoken failed: " + resp.text)
      self.accessDtAndToken = (dt, resp.text)
    
  def get(self, relPath:str, params = None, retryNr = 0) -> requests.Response:
    """API GET call

    Args:
        relPath (str): Path relative to the baseUrl.
        params (dict, optional): URL parameters. Defaults to None.
        retryNr (int, optional): How often the call has been tried already. Defaults to 0.

    Returns:
        requests.Response: API call response
    """
    if retryNr > self.maxRetries:
      raise RetryFailure()
    self._authenticate()
    try:
      return requests.get(self.apiUrl + relPath, params = params, headers = {'Authorization': 'Bearer ' + self.accessDtAndToken[1]}, timeout=self.timeoutS)
    except Exception as e:
      logging.debug("Retrying after unexpected exception: " + str(e))
      return self.get(relPath, params, retryNr+1)
  
  def post(self, relPath:str, data = None, json = None, params = None, retryNr = 0) -> requests.Response:
    """API POST call

    Args:
        relPath (str): Path relative to the baseUrl.
        json (dict, optional): JSON body. Defaults to None.
        params (dict, optional): URL parameters. Defaults to None.
        retryNr (int, optional): How often the call has been tried already. Defaults to 0.

    Returns:
        requests.Response: API call response
    """
    if retryNr > self.maxRetries:
      raise RetryFailure()
    self._authenticate()
    try:
        return requests.post(self.apiUrl + relPath, data = data, json = json, params = params, headers = {'Authorization': 'Bearer ' + self.accessDtAndToken[1]}, timeout=self.timeoutS)
    except Exception as e:
      logging.debug("Retrying after unexpected exception: " + str(e))
      return self.post(relPath, json, params, retryNr+1)
    
  def put(self, relPath:str, data = None, json = None, params = None, retryNr = 0) -> requests.Response:
    """API PUT call

    Args:
        relPath (str): Path relative to the baseUrl.
        json (dict, optional): JSON body. Defaults to None.
        params (dict, optional): URL parameters. Defaults to None.
        retryNr (int, optional): How often the call has been tried already. Defaults to 0.

    Returns:
        requests.Response: API call response
    """
    if retryNr > self.maxRetries:
      raise RetryFailure()
    self._authenticate()
    try:
      return requests.put(self.apiUrl + relPath, data = data, json = json, params = params, headers = {'Authorization': 'Bearer ' + self.accessDtAndToken[1]}, timeout=self.timeoutS)
    except Exception as e:
      logging.debug("Retrying after unexpected exception: " + str(e))
      return self.put(relPath, json, params, retryNr+1)
  
  def delete(self, relPath:str, params = None, retryNr = 0) -> requests.Response:
    """API DELETE call

    Args:
        relPath (str): Path relative to the baseUrl.
        params (dict, optional): URL parameters. Defaults to None.
        retryNr (int, optional): How often the call has been tried already. Defaults to 0.

    Returns:
        requests.Response: API call response
    """
    if retryNr > self.maxRetries:
      raise RetryFailure()
    self._authenticate()
    try:
      return requests.delete(self.apiUrl + relPath, params = params, headers = {'Authorization': 'Bearer ' + self.accessDtAndToken[1]}, timeout=self.timeoutS)
    except Exception as e:
      logging.debug("Retrying after unexpected exception: " + str(e))
      return self.delete(relPath, params, retryNr+1)

