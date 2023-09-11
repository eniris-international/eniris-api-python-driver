from typing import Callable, Optional, Union
from http import HTTPStatus

from requests import Session

from eniris.driver import retryRequest, DEFAULT_RETRY_CODES
from eniris.telemessage import Telemessage
from eniris.telemessage.writer.writer import TelemessageWriter


class DirectTelemessageWriterUnexpectedResponse(Exception):
    "Raised when the API responded with an unexpected status code"

    def __init__(self, code, message):
        super().__init__(f"Unexpected response [code: {code}]: {message}")
        self.code = code
        self.message = message


class DirectTelemessageWriter(TelemessageWriter):
    """Write telemessages (telemetry messages) to a specific endpoint in a blocking
    fashion: using this class to write messages will block a sending thread until the
    message is succesfully transmitted or an exception is raised.
    
    Note that you will typically need to specify some optional parameters to
    succesfully authenticate.

    Args:
      url (str, optional): The url to which the Telemessages will be posted.\
        Defaults to https://neodata-ingress.eniris.be/v1/telemetry
      params (dict[str, str], optional): A dictionary with fixed parameters which\
        should be included in each request. Defaults to an empty dictionary
      authorizationHeaderFunction (Callable|None, optional): A function returning a\
        valid authorization header, if None no authorization header is attached to\
          the request. Defaults to None
      timeoutS (int, optional): Request timeout in seconds. Defaults to 60
      maximumRetries (int, optional): How many times to try again in case of a\
        failure. Defaults to 4
      initialRetryDelayS (int, optional): The initial delay between successive\
        retries in seconds. Defaults to 1
      maximumRetryDelayS (int, optional): The maximum delay between successive\
        retries in seconds. Defaults to 60
      retryStatusCodes (set[int], optional): A set of all response code for which\
        a retry attempt must be made. Defaults to {429, 500, 503}
      session (requests.Session, optional): A session object to use for all calls.\
        If None, a requests.Session without extra options is created. Defaults to None
    """

    def __init__(
        self,
        url: str = "https://neodata-ingress.eniris.be/v1/telemetry",
        params: "Optional[dict[str, str]]" = None,
        authorizationHeaderFunction: "Union[Callable,None]" = None,
        timeoutS: float = 60,
        maximumRetries: int = 4,
        initialRetryDelayS: int = 1,
        maximumRetryDelayS: int = 60,
        retryStatusCodes: "Optional[set[int|HTTPStatus]]" = None,
        session: Optional[Session] = None,
    ):
        self.url = url
        self.params = {} if params is None else params
        self.authorizationHeaderFunction = authorizationHeaderFunction
        self.timeoutS = timeoutS
        self.maximumRetries = maximumRetries
        self.initialRetryDelayS = initialRetryDelayS
        self.maximumRetryDelayS = maximumRetryDelayS
        self.retryStatusCodes: "set[int|HTTPStatus]" = (
            DEFAULT_RETRY_CODES
            if retryStatusCodes is None
            else retryStatusCodes
        )
        self.session = Session() if session is None else session

    def writeTelemessage(self, message: Telemessage):
        """
        Write a single telemetry message to the API. This function is blocking: if it
        returns None, the message succesfully transmitted, if an unexpected response
        code is returned, a DirectTelemessageWriterUnexpectedResponse exception will be
        raised
        """
        res = retryRequest(
            self.session.post,
            self.url,
            params={**self.params, **message.parameters},
            data=b"\n".join(message.lines),
            authorizationHeaderFunction=self.authorizationHeaderFunction,
            timeout=self.timeoutS,
            maximumRetries=self.maximumRetries,
            initialRetryDelayS=self.initialRetryDelayS,
            maximumRetryDelayS=self.maximumRetryDelayS,
            retryStatusCodes=self.retryStatusCodes,
        )
        if res.status_code != 204:
            raise DirectTelemessageWriterUnexpectedResponse(res.status_code, res.text)
