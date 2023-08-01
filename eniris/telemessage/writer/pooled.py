import requests

from typing import Callable
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from threading import RLock, Condition, Thread
from heapq import heappop, heappush
import logging

from eniris.telemessage import Telemessage
from eniris.telemessage.writer import TelemessageWriter
from http import HTTPStatus

@dataclass
class TelemessageWrapper:
  telemessage: Telemessage
  creationDt: datetime
  scheduledDt: datetime
  retryNr: int
  isFinished: bool

  def __init__(self, telemessage: Telemessage, creationDt:datetime=None, scheduledDt:datetime=None, retryNr:int=0):
    self.lock = RLock()
    self.telemessage = telemessage
    self.creationDt = datetime.now(timezone.utc) if creationDt is None else creationDt
    self.scheduledDt = datetime.now(timezone.utc) if scheduledDt is None else scheduledDt
    self.retryNr = retryNr
    self.finishedCondition = Condition()
    self.isFinished = False

  def reschedule(self, reason: str, queue):
    with self.lock:
      if self.retryNr+1 <= queue.maximumRetries:
        logging.warning(f"Retrying request after {reason}")
        self.scheduledDt = datetime.now(timezone.utc) + timedelta(seconds=min(queue.initialRetryDelayS*2**self.retryNr, queue.maximumRetryDelayS))
        self.retryNr += 1
      else:
        logging.exception(f"Maximum number of retries exceeded, dropping request due to {reason}")
        self.complete()
        return
    with queue.lock:
      queue.removeActive(self)
      queue.pushWaiting(self)

  def complete(self):
    with self.lock:
      self.isFinished = True
      self.finishedCondition.notifyAll()

  def wait(self):
    with self.lock:
      if not self.isFinished:
        self.finishedCondition.wait()

  def __lt__(self, other):
        return self.scheduledDt < other.scheduledDt
  
  def __eq__(self, other):
        return self.scheduledDt == other.scheduledDt

class TelemessageWrapperQueue:
  def __init__(self, waitingMessages: list[TelemessageWrapper]=[], maximumRetries:int=4, initialRetryDelayS:int=1, maximumRetryDelayS:int=60, retryStatusCodes:set[int]=set([HTTPStatus.TOO_MANY_REQUESTS,HTTPStatus.INTERNAL_SERVER_ERROR,HTTPStatus.SERVICE_UNAVAILABLE])):
    self.lock = RLock()
    self.waitingMessages = waitingMessages
    self.maximumRetries = maximumRetries
    self.initialRetryDelayS = initialRetryDelayS
    self.maximumRetryDelayS = maximumRetryDelayS
    self.retryStatusCodes = retryStatusCodes
    self.activeMessages: set[TelemessageWrapper] = set()
    self.newFirstWaitingMessage: Condition = Condition(self.lock)

  def removeActive(self, tmw: TelemessageWrapper):
    with self.lock:
      self.activeMessages.remove(tmw)

  def popWaitingAndAddToActive(self) -> TelemessageWrapper:
    with self.lock:
      tmw = heappop(self.waitingMessages)
      self.activeMessages.add(tmw)
      return tmw
      
  def pushWaiting(self, tmw: TelemessageWrapper):
    with self.lock:
      currentFirstWaitingMessage = self.waitingMessages[0] if len(self.waitingMessages) > 0 else None
      heappush(self.waitingMessages, tmw)
      if currentFirstWaitingMessage is None or tmw.scheduledDt < currentFirstWaitingMessage.scheduledDt:
        self.newFirstWaitingMessage.notifyAll()

  def content(self):
    with self.lock:
      return list(self.waitingMessages) + list(self.activeMessages)

class PooledTelemessageWriterDaemon(Thread):
  def __init__(self, queue: TelemessageWrapperQueue,
               url:str="https://neodata-ingress.eniris.be/v1/telemetry", params:dict[str, str]={}, authorizationHeaderFunction:Callable|None=None, timeoutS:float=60,
               session:requests.Session=None):
    super().__init__()
    self.queue = queue
    self.url = url
    self.params = params
    self.authorizationHeaderFunction = authorizationHeaderFunction
    self.timeoutS = timeoutS
    self.session = requests.Session() if session is None else session

  def run(self):
    logging.debug("Started PooledTelemessageWriterDaemon")
    while True:
      with self.queue.lock:
        while True:
          tmw = self.queue.popWaitingAndAddToActive()
          session = self.session
          if session is None or tmw is not None:
            break
          print("sleep")
          self.queue.newFirstWaitingMessage.wait()
          print("awake")
      print("haha")
      if session is None:
        logging.debug("Stopped PooledTelemessageWriterDaemon")
        return
      try:
        headers: dict[str, str] = {"Authorization": self.authorizationHeaderFunction()} if self.authorizationHeaderFunction is not None else {}
        resp = session.post(self.url, headers=headers, params=self.params|tmw.telemessage.parameters, timeout=self.timeoutS)
        if resp.status_code == 204:
          tmw.complete()
        elif resp.status_code in self.retryStatusCodes:
          tmw.reschedule(f"response with status code {resp.status_code} ({HTTPStatus(resp.status_code).phrase}): {resp.text}", self.queue)
        else:
          logging.exception(f"Dropping request due to response with status code {resp.status_code} ({HTTPStatus(resp.status_code).phrase}): {resp.text}")
      except requests.Timeout:
         tmw.reschedule(f"timeout", self.queue)

  def stop(self):
    with self.queue.lock:
      self.session = None
      self.queue.newFirstWaitingMessage.notifyAll()

class PooledTelemessageWriter(TelemessageWriter):
  """Write telemessages (telemetry messages) to a specific endpoint in a blocking fashion:
  using this class to write messages will block a sending thread until the message is succesfully transmitted or an exception is raised.
  """
  def __init__(self, poolSize:int=1,
               minimumSnaphotAgeS:int=5, maximumSnapshotSizeBytes=20_000_000, snapshotFolder:str|None=None,
               url:str="https://neodata-ingress.eniris.be/v1/telemetry", params:dict[str, str]={}, authorizationHeaderFunction:Callable|None=None, timeoutS:float=60, session:requests.Session=None,
               maximumRetries:int=4, initialRetryDelayS:int=1, maximumRetryDelayS:int=60, retryStatusCodes:set[int]=set([HTTPStatus.TOO_MANY_REQUESTS,HTTPStatus.INTERNAL_SERVER_ERROR,HTTPStatus.SERVICE_UNAVAILABLE])
              ):
    """Constructor. Note that you will typically need to specify some optional parameters to succesfully authenticate

    Args:
        url (str, optional): The url to which the Telemessages will be posted. Defaults to https://neodata-ingress.eniris.be/v1/telemetry
        params (dict[str, str], optional): A dictionary with fixed parameters which should be included in each request. Defaults to an empty dictionary
        authorizationHeaderFunction (Callable|None, optional): A function returning a valid authorization header, if None no authorization header is attached to the request. Defaults to None
        timeoutS (int, optional): Request timeout in seconds. Defaults to 60
        session (requests.Session, optional): A session object to use for all calls. If None, a requests.Session without extra options is created. Defaults to None
        maximumRetries (int, optional): How many times to try again in case of a failure. Defaults to 4
        initialRetryDelayS (int, optional): The initial delay between successive retries in seconds. Defaults to 1
        maximumRetryDelayS (int, optional): The maximum delay between successive retries in seconds. Defaults to 60
        retryStatusCodes (set[str], optional): A set of all response code for which a retry attempt must be made. Defaults to {429, 500, 503}
    """
    self.telemessageWrapperQueue = TelemessageWrapperQueue(maximumRetries=maximumRetries, initialRetryDelayS=initialRetryDelayS, maximumRetryDelayS=maximumRetryDelayS, retryStatusCodes=retryStatusCodes)
    session = requests.Session() if session is None else session
    self.pool = [PooledTelemessageWriterDaemon(self.telemessageWrapperQueue,
      url=url, params=params, authorizationHeaderFunction=authorizationHeaderFunction, timeoutS=timeoutS,
      session=session) for i in range(poolSize)]
    for d in self.pool:
      d.start()
  
  def writeTelemessage(self, tm: Telemessage):
    """
    Write a single telemetry message to the API
    """
    self.telemessageWrapperQueue.pushWaiting(TelemessageWrapper(tm))

  def flush(self):
    for tmw in self.telemessageWrapperQueue.content():
      tmw.wait()

  def __del__(self):
    self.flush()
    with self.telemessageWrapperQueue.lock:
      for d in self.pool:
        d.stop()