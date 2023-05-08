from typing import Union, Callable
import datetime
import requests
import logging
import time
from http import HTTPStatus

class AuthenticationFailure(Exception):
    "Raised when failing to authentiate to the Insights API"
    pass

def checkRetryCondition(response: requests.Response) -> bool:
  """ Checks a HTTP response for status codes that indicate a temporary fault condition. Mainly responses that indicate a temporary server failure. For these conditions it makes sense to retry the HTTP request later.
  
  Args:
    response (requests.Response)

  Returns:
    bool: True if the HTTP response contains an error status code that corresponds with a temporary server failure.
  """
  RETRY_STATUS_CODES = {
    HTTPStatus.TOO_MANY_REQUESTS,
    HTTPStatus.INTERNAL_SERVER_ERROR,
    HTTPStatus.SERVICE_UNAVAILABLE
  }
  return response.status_code in RETRY_STATUS_CODES


# Provide an easy interface to interact with the API, in the style of the requests library (https://docs.python-requests.org/en/master/)
class ApiDriver:
  def __init__(self, username:str, password:str, authUrl:str = 'https://authentication.eniris.be', apiUrl:str = 'https://api.eniris.be', timeoutS:int = 60, maximumRetries:int = 4, initialRetryDelayS:int=1, maximumRetryDelayS:int=60, session: requests.Session=None):
    """Constructor. You must specify at least username and password or a credentialsPath where they are stored as a json of the form {'login': USERNAME, 'password': PASSWORD}

    Args:
        username (str, optional): Insights username
        password (str, optional): Insights password of the user
        authUrl (str, optional): Url of authentication endpoint. Defaults to https://authentication.eniris.be
        apiUrl (str, optional): Url of api endpoint. Defaults to https://api.eniris.be
        timeoutS (int, optional): API timeout in seconds. Defaults to 60
        maximumRetries (int, optional): How many times to try again in case of a failure. Defaults to 4
        initialRetryDelayS (int, optional): The initial delay between successive retries in seconds. Defaults to 1
        maximumRetryDelayS (int, optional): The maximum delay between successive retries in seconds. Defaults to 60
        session (requests.Session, optional): A session object to use for all API calls. If None, a requests.Session without extra options is created. Defaults to None

    Raises:
        Exception: _description_
    """
    self.username = username
    self.password = password
    self.authUrl = authUrl
    self.apiUrl = apiUrl
    self.timeoutS = timeoutS
    self.maximumRetries = maximumRetries
    self.initialRetryDelayS = initialRetryDelayS
    self.maximumRetryDelayS = maximumRetryDelayS
    self.refreshDtAndToken = None
    self.accessDtAndToken = None
    self.session = requests.Session() if session is None else session
    
  def _authenticate(self):
    dt = datetime.datetime.now()
    if self.refreshDtAndToken is None or (dt - self.refreshDtAndToken[0]).total_seconds() > 13*24*60*60: # 13 days
      data = { "username": self.username, "password": self.password }
      resp = self.session.post(self.authUrl + '/auth/login', json = data, timeout=self.timeoutS)
      if resp.status_code != 200:
        raise AuthenticationFailure("login failed: " + resp.text)
      self.refreshDtAndToken = (dt, resp.text)
    elif (dt - self.refreshDtAndToken[0]).total_seconds() > 7*24*60*60: # 7 days
      resp = self.session.get(self.authUrl + '/auth/refreshtoken', headers = {'Authorization': 'Bearer ' + self.refreshDtAndToken[1]}, timeout=self.timeoutS)
      if resp.status_code == 200:
        self.refreshDtAndToken = (dt, resp.text)
      else:
        # Not the biggest problem, sice the refresh token will still be valid for a while, but we should log an exception
        logging.warning("Unable to renew the refresh token: " + resp.text)
    if self.accessDtAndToken is None or (dt - self.accessDtAndToken[0]).total_seconds() > 2*60: # 2 minutes
      resp = self.session.get(self.authUrl + '/auth/accesstoken', headers = {'Authorization': 'Bearer ' + self.refreshDtAndToken[1]}, timeout=self.timeoutS)
      if resp.status_code != 200:
        raise AuthenticationFailure("accesstoken failed: " + resp.text)
      self.accessDtAndToken = (dt, resp.text)
      
  def close(self):
    """Log out from the API
    """
    dt = datetime.datetime.now()
    if self.refreshDtAndToken is None or (dt - self.refreshDtAndToken[0]).total_seconds() > 14*24*60*60: # 14 days
      # The refresh token did already expire, there is no reason to log out
      return
    resp = self.session.post(self.authUrl + '/auth/logout', headers = {'Authorization': 'Bearer ' + self.refreshDtAndToken[1]}, timeout=self.timeoutS)
    if resp.status_code == 204 or resp.status_code == 401:
      # The refresh token was either succesfully added to the deny list, or it was already invalid
      self.refreshDtAndToken = None
      self.accessDtAndToken = None 
    else:
      raise AuthenticationFailure("logout failed: " + resp.text)
  
  def __retry(self, requests_function:Callable, path:str, retryNr = 0, **req_function_kwargs) -> requests.Response:
    """Execute the given requests_function with the provided req_function_kwargs keyword arguments. If the function fails, it will try again until the amount of retries has exceeded.

    Args:
        requests_function (Callable): Requests function to use. Can be self.session.get, self.session.post, self.session.put or self.session.delete
        path (str): Path relative to the apiUrl.
        retryNr (int, optional): How often the call has been tried already. Defaults to 0.
        req_function_kwargs (dict): Keyword arguments for the requests_function.

    Returns:
        requests.Response: HTTP response
    """
    req_path = path if path.startswith('http://') or path.startswith('https://') else self.apiUrl + path
    try:
      resp = requests_function(req_path, **req_function_kwargs)
    except requests.Timeout as e:
      resp = e
    if isinstance(resp, requests.Timeout):
      if retryNr+1 <= self.maximumRetries:
        logging.warning("Retrying API call after timeout.")
        time.sleep(min(self.initialRetryDelayS*2**retryNr, self.maximumRetryDelayS))
        resp = self.__retry(requests_function, path, retryNr+1, **req_function_kwargs)
      else:
        raise requests.Timeout(resp)
    elif (checkRetryCondition(resp)) and retryNr+1 <= self.maximumRetries:
        logging.warning(f'Retrying API call after response with status code {resp.status_code} ({HTTPStatus(resp.status_code).phrase}): {resp.text}')
        time.sleep(min(self.initialRetryDelayS*2**retryNr, self.maximumRetryDelayS))
        resp = self.__retry(requests_function, path, retryNr+1, **req_function_kwargs)
    return resp
      
  def get(self, path:str, params = None, **kwargs) -> requests.Response:
    """API GET call

    Args:
        path (str): Path relative to the apiUrl.
        params (dict, optional): URL parameters. Defaults to None.

    Returns:
        requests.Response: API call response
    """
    self._authenticate()
    return self.__retry(self.session.get, path, params = params, headers = {'Authorization': 'Bearer ' + self.accessDtAndToken[1]}, timeout=self.timeoutS)

  def post(self, path:str, json = None, params = None, data = None, **kwargs) -> requests.Response:
    """API POST call()

    Args:
        path (str): Path relative to the apiUrl.
        json (dict, optional): JSON body. Defaults to None.
        params (dict, optional): URL parameters. Defaults to None.

    Returns:
        requests.Response: API call response
    """
    self._authenticate()
    return self.__retry(self.session.post, path, json = json, params = params, data = data, headers = {'Authorization': 'Bearer ' + self.accessDtAndToken[1]}, timeout=self.timeoutS)
    
  def put(self, path:str, json = None, params = None, data = None, **kwargs) -> requests.Response:
    """API PUT call

    Args:
        path (str): Path relative to the apiUrl.
        json (dict, optional): JSON body. Defaults to None.
        params (dict, optional): URL parameters. Defaults to None.

    Returns:
        requests.Response: API call response
    """
    self._authenticate()
    return self.__retry(self.session.put, path, json = json, params = params, data = data, headers = {'Authorization': 'Bearer ' + self.accessDtAndToken[1]}, timeout=self.timeoutS)
  
  def delete(self, path:str, params = None, **kwargs) -> requests.Response:
    """API DELETE call

    Args:
        path (str): Path relative to the baseUrl.
        params (dict, optional): URL parameters. Defaults to None.

    Returns:
        requests.Response: API call response
    """
    self._authenticate()
    return self.__retry(self.session.delete, path, params = params, headers = {'Authorization': 'Bearer ' + self.accessDtAndToken[1]}, timeout=self.timeoutS)