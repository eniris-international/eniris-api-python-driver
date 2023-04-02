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
      
  def close(self):
    """Log out from the API
    """
    if self.refreshDtAndToken is None or (dt - self.refreshDtAndToken[0]).total_seconds() > 14*24*60*60: # 14 days
      # The refresh token did already expire, there is no reason to log out
      return
    resp = requests.post(self.authUrl + '/auth/logout', headers = {'Authorization': 'Bearer ' + self.refreshDtAndToken[1]}, timeout=self.timeoutS)
    if resp.status_code == 204 or resp.status_code == 401:
      # The refresh token was either succesfully added to the deny list, or it was already invalid
      self.refreshDtAndToken = None
      self.accessDtAndToken = None 
    else:
      raise AuthenticationFailure("logout failed: " + resp.text)
      
  def get(self, path:str, params = None, retryNr = 0) -> requests.Response:
    """API GET call

    Args:
        path (str): Path relative to the apiUrl.
        params (dict, optional): URL parameters. Defaults to None.
        retryNr (int, optional): How often the call has been tried already. Defaults to 0.

    Returns:
        requests.Response: API call response
    """
    if retryNr > self.maxRetries:
      raise RetryFailure()
    self._authenticate()
    try:
      req_path = path if path.startswith('http://') or path.startswith('https://') else self.apiUrl + path
      return requests.get(req_path, params = params, headers = {'Authorization': 'Bearer ' + self.accessDtAndToken[1]}, timeout=self.timeoutS)
    except Exception as e:
      logging.debug("Retrying after unexpected exception: " + str(e))
      return self.get(path, params, retryNr+1)
  
  def post(self, path:str, json = None, params = None, data = None, retryNr = 0) -> requests.Response:
    """API POST call

    Args:
        path (str): Path relative to the apiUrl.
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
      req_path = path if path.startswith('http://') or path.startswith('https://') else self.apiUrl + path
      return requests.post(req_path, json = json, params = params, data = data, headers = {'Authorization': 'Bearer ' + self.accessDtAndToken[1]}, timeout=self.timeoutS)
    except Exception as e:
      logging.debug("Retrying after unexpected exception: " + str(e))
      return self.post(path, json, params, data, retryNr+1)
    
  def put(self, path:str, json = None, params = None, data = None, retryNr = 0) -> requests.Response:
    """API PUT call

    Args:
        path (str): Path relative to the apiUrl.
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
      req_path = path if path.startswith('http://') or path.startswith('https://') else self.apiUrl + path
      return requests.put(req_path, json = json, params = params, data = data, headers = {'Authorization': 'Bearer ' + self.accessDtAndToken[1]}, timeout=self.timeoutS)
    except Exception as e:
      logging.debug("Retrying after unexpected exception: " + str(e))
      return self.put(path, json, params, data, retryNr+1)
  
  def delete(self, path:str, params = None, retryNr = 0) -> requests.Response:
    """API DELETE call

    Args:
        path (str): Path relative to the baseUrl.
        params (dict, optional): URL parameters. Defaults to None.
        retryNr (int, optional): How often the call has been tried already. Defaults to 0.

    Returns:
        requests.Response: API call response
    """
    if retryNr > self.maxRetries:
      raise RetryFailure()
    self._authenticate()
    try:
      req_path = path if path.startswith('http://') or path.startswith('https://') else self.apiUrl + path
      return requests.delete(req_path, params = params, headers = {'Authorization': 'Bearer ' + self.accessDtAndToken[1]}, timeout=self.timeoutS)
    except Exception as e:
      logging.debug("Retrying after unexpected exception: " + str(e))
      return self.delete(path, params, retryNr+1)

