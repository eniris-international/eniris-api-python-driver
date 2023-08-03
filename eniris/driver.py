#!/usr/bin/python
from typing import Callable
import datetime
import requests
import logging
import time
from threading import RLock
from http import HTTPStatus

class AuthenticationFailure(Exception):
    "Raised when failing to authentiate to the Insights API"
    pass

def retryRequest(requestsFunction:Callable, path:str, authorizationHeaderFunction: Callable|None = None,
            maximumRetries:int = 4, initialRetryDelayS:int=1, maximumRetryDelayS:int=60, retryStatusCodes:set[int]=set([HTTPStatus.TOO_MANY_REQUESTS,HTTPStatus.INTERNAL_SERVER_ERROR,HTTPStatus.SERVICE_UNAVAILABLE]), retryNr:int = 0,
            **req_function_kwargs) -> requests.Response:
  """Execute the given requests_function with the provided req_function_kwargs keyword arguments. If the function fails, it will try again until the amount of retries has exceeded.

  Args:
      requestsFunction (Callable): Requests function to use. Can be self.session.get, self.session.post, self.session.put or self.session.delete
      path (str): Url to use with the requests function
      authorizationHeaderFunction (Callable, optional): A function returning a valid authorization header, if None no authorization header is attached to the request. Defaults to None
      maximumRetries (int, optional): How many times to try again in case of a failure. Defaults to 4
      initialRetryDelayS (int, optional): The initial delay between successive retries in seconds. Defaults to 1
      maximumRetryDelayS (int, optional): The maximum delay between successive retries in seconds. Defaults to 60
      retryStatusCodes (set[str], optional): A set of all response code for which a retry attempt must be made. Defaults to {429, 500, 503}
      retryNr (int, optional): How often the call has been tried already. Defaults to 0.
      req_function_kwargs (dict): Keyword arguments for the requests_function.

  Returns:
      requests.Response: HTTP response
  """
  try:
    headers: dict[str, str] = req_function_kwargs.get("headers", dict())
    if authorizationHeaderFunction is not None:
      headers["Authorization"] = authorizationHeaderFunction()
    req_function_kwargs["headers"] = headers
    resp = requestsFunction(path, **req_function_kwargs)
  except requests.Timeout as e:
    resp = e
  except requests.ConnectionError as e:
    resp = e
  if isinstance(resp, Exception):
    if retryNr+1 <= maximumRetries:
      logging.warning(f"Retrying request after exception: {resp}")
      time.sleep(min(initialRetryDelayS*2**retryNr, maximumRetryDelayS))
      resp = retryRequest(requestsFunction, path, authorizationHeaderFunction,
                     maximumRetries, initialRetryDelayS, maximumRetryDelayS, retryStatusCodes, retryNr+1,
                     **req_function_kwargs)
    else:
      raise resp
  elif resp.status_code in retryStatusCodes and retryNr+1 <= maximumRetries:
      logging.warning(f'Retrying request after response with status code {resp.status_code} ({HTTPStatus(resp.status_code).phrase}): {resp.text}')
      time.sleep(min(initialRetryDelayS*2**retryNr, maximumRetryDelayS))
      resp = retryRequest(requestsFunction, path, authorizationHeaderFunction,
                     maximumRetries, initialRetryDelayS, maximumRetryDelayS, retryStatusCodes, retryNr+1,
                     **req_function_kwargs)
  return resp



class ApiDriver:
  """ An easy thread-save interface to interact with the API, with get, post, put and delete methods in the style of the requests library (https://docs.python-requests.org/en/master/) """
  def __init__(self, username:str, password:str,
               authUrl:str = 'https://authentication.eniris.be', apiUrl:str = 'https://api.eniris.be', timeoutS:int = 60,
               maximumRetries:int = 4, initialRetryDelayS:int=1, maximumRetryDelayS:int=60, retryStatusCodes:set[int]=set([HTTPStatus.TOO_MANY_REQUESTS,HTTPStatus.INTERNAL_SERVER_ERROR,HTTPStatus.SERVICE_UNAVAILABLE]),
               session: requests.Session=None):
    """Constructor. You must specify at least a username and password

    Args:
        username (str, optional): Insights username
        password (str, optional): Insights password of the user
        authUrl (str, optional): Url of authentication endpoint. Defaults to https://authentication.eniris.be
        apiUrl (str, optional): Url of api endpoint. Defaults to https://api.eniris.be Use https://neodata-ingress.eniris.be/v1/telemetry when writing telemetry
        timeoutS (int, optional): API timeout in seconds. Defaults to 60
        maximumRetries (int, optional): How many times to try again in case of a failure. Defaults to 4
        initialRetryDelayS (int, optional): The initial delay between successive retries in seconds. Defaults to 1
        maximumRetryDelayS (int, optional): The maximum delay between successive retries in seconds. Defaults to 60
        retryStatusCodes (set[str], optional): A set of all response code for which a retry attempt must be made. Defaults to {429, 500, 503}
        session (requests.Session, optional): A session object to use for all API calls. If None, a requests.Session without extra options is created. Defaults to None
    """
    self.username = username
    self.password = password
    self.authUrl = authUrl
    self.apiUrl = apiUrl
    self.timeoutS = timeoutS
    self.maximumRetries = maximumRetries
    self.initialRetryDelayS = initialRetryDelayS
    self.maximumRetryDelayS = maximumRetryDelayS
    self.retryStatusCodes = retryStatusCodes
    self.refreshTokenLock = RLock()
    self.refreshDtAndToken = None
    self.accessTokenLock = RLock()
    self.accessDtAndToken = None
    self.session = requests.Session() if session is None else session

  def refreshtoken(self):
    """Get a refresh token to authenticate with the API

    Returns:
        string: A refresh token of the format `Bearer token`
    """
    with self.refreshTokenLock:
      dt = datetime.datetime.now()
      if self.refreshDtAndToken is None or (dt - self.refreshDtAndToken[0]).total_seconds() > 13*24*60*60: # 13 days
        data = { "username": self.username, "password": self.password }
        try:
          resp = retryRequest(self.session.post, f'{self.authUrl}/auth/login', json = data, timeout=self.timeoutS,
                        maximumRetries=self.maximumRetries, initialRetryDelayS=self.initialRetryDelayS, maximumRetryDelayS=self.maximumRetryDelayS, retryStatusCodes=self.retryStatusCodes)
          if resp.status_code != 200:
            raise AuthenticationFailure(f"Unable to login: {resp.text}")
        except requests.Timeout:
            raise AuthenticationFailure(f"Unable to login: the API did not respond in time")
        self.refreshDtAndToken = (dt, resp.text)
      elif (dt - self.refreshDtAndToken[0]).total_seconds() > 7*24*60*60: # 7 days
        try:
          # No retries here, since this is not critical...
          resp = self.session.get(f"{self.authUrl}/auth/refreshtoken", headers = {'Authorization': f"Bearer {self.refreshDtAndToken[1]}"}, timeout=self.timeoutS)
          if resp.status_code == 200:
            self.refreshDtAndToken = (dt, resp.text)
          else:
            # Not the biggest problem, sice the refresh token will still be valid for a while, but we should log an exception
            logging.warning(f"Unable to renew the refresh token: {resp.text}")
        except requests.Timeout:
            # Not the biggest problem, sice the refresh token will still be valid for a while, but we should log an exception
            logging.warning(f"Unable to renew the refresh token: the API did not respond in time")
      return f"Bearer {self.refreshDtAndToken[1]}"
    
  def accesstoken(self):
    """Get an access token to authenticate with the API

    Returns:
        string: An access token of the format `Bearer token`
    """
    with self.accessTokenLock:
      dt = datetime.datetime.now()
      if self.accessDtAndToken is None or (dt - self.accessDtAndToken[0]).total_seconds() > 2*60: # 2 minutes
        try:
          resp = retryRequest(self.session.get, f'{self.authUrl}/auth/accesstoken', authorizationHeaderFunction=self.refreshtoken, timeout=self.timeoutS,
                        maximumRetries=self.maximumRetries, initialRetryDelayS=self.initialRetryDelayS, maximumRetryDelayS=self.maximumRetryDelayS, retryStatusCodes=self.retryStatusCodes)
          if resp.status_code != 200:
            raise AuthenticationFailure(f"Unable to collect an access token: {resp.text}")
        except requests.Timeout:
            raise AuthenticationFailure(f"Unable to collect an access token: the API did not respond in time")
        self.accessDtAndToken = (dt, resp.text)
      return f"Bearer {self.accessDtAndToken[1]}"
      
  def close(self):
    """Log out from the API
    """
    dt = datetime.datetime.now()
    if self.refreshDtAndToken is None or (dt - self.refreshDtAndToken[0]).total_seconds() > 14*24*60*60: # 14 days
      # The refresh token did already expire, there is no reason to log out
      return
    try:
      resp = retryRequest(self.session.post, f'{self.authUrl}/auth/logout', authorizationHeaderFunction=self.refreshtoken, timeout=self.timeoutS,
                    maximumRetries=self.maximumRetries, initialRetryDelayS=self.initialRetryDelayS, maximumRetryDelayS=self.maximumRetryDelayS, retryStatusCodes=self.retryStatusCodes)
      if resp.status_code == 204 or resp.status_code == 401:
        # The refresh token was either succesfully added to the deny list, or it was already invalid
        self.refreshDtAndToken = None
        self.accessDtAndToken = None 
      else:
        raise AuthenticationFailure(f"Unable to logout: {resp.text}")
    except requests.Timeout:
        raise AuthenticationFailure(f"Unable to logout: the API did not respond in time")
      
  def get(self, path:str, params = None, **kwargs) -> requests.Response:
    """API GET call

    Args:
        path (str): Path relative to the apiUrl or a full url
        params (dict, optional): URL parameters. Defaults to None.

    Returns:
        requests.Response: API call response
    """
    path = path if path.startswith('http://') or path.startswith('https://') else self.apiUrl + path
    return retryRequest(self.session.get, path, params = params, authorizationHeaderFunction=self.accesstoken, timeout=self.timeoutS,
            maximumRetries=self.maximumRetries, initialRetryDelayS=self.initialRetryDelayS, maximumRetryDelayS=self.maximumRetryDelayS, retryStatusCodes=self.retryStatusCodes)

  def post(self, path:str, json = None, params = None, data = None, **kwargs) -> requests.Response:
    """API POST call()

    Args:
        path (str): Path relative to the apiUrl or a full url
        json (dict, optional): JSON body. Defaults to None.
        params (dict, optional): URL parameters. Defaults to None.

    Returns:
        requests.Response: API call response
    """
    path = path if path.startswith('http://') or path.startswith('https://') else self.apiUrl + path
    return retryRequest(self.session.post, path, json = json, params = params, data = data, authorizationHeaderFunction=self.accesstoken, timeout=self.timeoutS,
            maximumRetries=self.maximumRetries, initialRetryDelayS=self.initialRetryDelayS, maximumRetryDelayS=self.maximumRetryDelayS, retryStatusCodes=self.retryStatusCodes)
    
  def put(self, path:str, json = None, params = None, data = None, **kwargs) -> requests.Response:
    """API PUT call

    Args:
        path (str): Path relative to the apiUrl or a full url
        json (dict, optional): JSON body. Defaults to None.
        params (dict, optional): URL parameters. Defaults to None.

    Returns:
        requests.Response: API call response
    """
    path = path if path.startswith('http://') or path.startswith('https://') else self.apiUrl + path
    return retryRequest(self.session.put, path, json = json, params = params, data = data, authorizationHeaderFunction=self.accesstoken, timeout=self.timeoutS,
            maximumRetries=self.maximumRetries, initialRetryDelayS=self.initialRetryDelayS, maximumRetryDelayS=self.maximumRetryDelayS, retryStatusCodes=self.retryStatusCodes)
  
  def delete(self, path:str, params = None, **kwargs) -> requests.Response:
    """API DELETE call

    Args:
        path (str): Path relative to the apiUrl or a full url
        params (dict, optional): URL parameters. Defaults to None.

    Returns:
        requests.Response: API call response
    """
    path = path if path.startswith('http://') or path.startswith('https://') else self.apiUrl + path
    return retryRequest(self.session.delete, path, params = params, authorizationHeaderFunction=self.accesstoken, timeout=self.timeoutS,
            maximumRetries=self.maximumRetries, initialRetryDelayS=self.initialRetryDelayS, maximumRetryDelayS=self.maximumRetryDelayS, retryStatusCodes=self.retryStatusCodes)