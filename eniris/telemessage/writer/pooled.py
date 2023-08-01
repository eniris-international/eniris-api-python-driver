import requests

from typing import Callable, ClassVar
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from threading import Lock, RLock, Condition, Thread
from heapq import heappop, heappush
import os, pickle, re, logging, time

from eniris.telemessage import Telemessage
from eniris.telemessage.writer import TelemessageWriter
from http import HTTPStatus

@dataclass
class TelemessageWrapper:
  telemessage: Telemessage
  creationDt: datetime
  subId: int
  _subIdCntLock: ClassVar[Lock] = Lock()
  _subIdCnt: ClassVar[int] = 0

  @staticmethod
  def incrementSubId():
    with TelemessageWrapper._subIdCntLock:
      TelemessageWrapper._subIdCnt += 1
      return TelemessageWrapper._subIdCnt
    
  @staticmethod
  def loadSnapshot(path:str):
    filename = os.path.basename(path)
    m = re.match('([^_]*)_subId_([^_]*)_scheduledDt_([^_]*)_retryNr_(\d+).pickle', filename)
    if not m:
      raise ValueError(f"Unable to parse filename {filename}")
    with open(path, 'rb') as f:
      telemessage: Telemessage = pickle.load(f)
    creationDt = datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S.%f%z")
    subId = int(m.group(2))
    scheduledDt = datetime.strptime(m.group(3), "%Y-%m-%dT%H:%M:%S.%f%z")
    retryNr = int(m.group(4))
    return TelemessageWrapper(telemessage, creationDt, subId, scheduledDt, retryNr, path)
    
  def filename(self):
    return f"{self.creationDt.isoformat()}_subId_{self.subId}_scheduledDt_{self.scheduledDt.isoformat()}_retryNr_{self.retryNr}.pickle"

  def __init__(self, telemessage: Telemessage, creationDt:datetime=None, subId:int=None, scheduledDt:datetime=None, retryNr:int=0, snapshotPath: str=None):
    self._lock = RLock()
    self.telemessage = telemessage
    self.creationDt = datetime.now(timezone.utc) if creationDt is None else creationDt
    self.subId = TelemessageWrapper.incrementSubId() if subId is None else subId
    self._scheduledDt = datetime.now(timezone.utc) if scheduledDt is None else scheduledDt
    self._retryNr = retryNr
    self._snapshotPath = snapshotPath
    self._finishedCondition = Condition(self._lock)
    self._isFinished = False

  @property
  def id(self):
    return f"{self.creationDt.isoformat()}#{self.subId}"
  
  def saveSnapshot(self, dirname:str):
    with self._lock:
      snapshotPath = os.path.join(dirname, self.filename())
      with open(snapshotPath, 'w') as f:
        logging.debug(f"Writing Telemessage to '{snapshotPath}'")
        pickle.dump(self.telemessage, f)
      self._snapshotPath = snapshotPath

  def reschedule(self, reason: str, queue):
    with self._lock:
      if self._retryNr+1 <= queue.maximumRetries:
        logging.warning(f"Retrying request after {reason}")
        self._scheduledDt = datetime.now(timezone.utc) + timedelta(seconds=min(queue.initialRetryDelayS*2**self._retryNr, queue.maximumRetryDelayS))
        self._retryNr += 1
        if self._snapshotPath:
          os.rename(self._snapshotPath, os.path.join(os.path.dirname(self._snapshotPath), self.filename()))
      else:
        logging.exception(f"Maximum number of retries exceeded, dropping request due to {reason}")
        self.complete()
        return
    queue.removeActive(self)
    queue.pushWaiting(self)

  def complete(self):
    with self._lock:
      self._isFinished = True
      self._finishedCondition.notifyAll()
      if self._snapshotPath:
        os.remove(self._snapshotPath)

  def wait(self):
    with self._lock:
      if not self._isFinished:
        self._finishedCondition.wait()

  # Note, we assume that no other threads are trying to modify the scheduledDt when comparing elements
  def __lt__(self, other):
        return self._scheduledDt < other._scheduledDt
  
  # Note, we assume that no other threads are trying to modify the scheduledDt when comparing elements
  def __eq__(self, other):
        return self._scheduledDt == other._scheduledDt

class TelemessageWrapperQueue:
  def __init__(self, waitingMessages: list[TelemessageWrapper]=[], maximumRetries:int=4, initialRetryDelayS:int=1, maximumRetryDelayS:int=60, retryStatusCodes:set[int]=set([HTTPStatus.TOO_MANY_REQUESTS,HTTPStatus.INTERNAL_SERVER_ERROR,HTTPStatus.SERVICE_UNAVAILABLE])):
    self.maximumRetries = maximumRetries
    self.initialRetryDelayS = initialRetryDelayS
    self.maximumRetryDelayS = maximumRetryDelayS
    self.retryStatusCodes = retryStatusCodes
    self._lock = RLock()
    self._activeMessages: dict[str, TelemessageWrapper] = dict()
    self._waitingMessages = list(waitingMessages)
    self._moreMessagesOrStoppingCondition: Condition = Condition(self._lock)
    self._isStopping = False

  def removeActive(self, tmw: TelemessageWrapper):
    with self._lock:
      del self._activeMessages[tmw.id]

  def popWaitingAndAddToActive(self):
    with self._lock:
      while True:
        if len(self._waitingMessages) > 0:
          tmw = self._waitingMessages[0]
          sleepTimeS = tmw._scheduledDt.timestamp() - time.time()
          if sleepTimeS < 0:
            self._moreMessagesOrStoppingCondition.notify()
            heappop(self._waitingMessages)
            self._activeMessages[tmw.id] = tmw
            return tmw
          else:
            self._moreMessagesOrStoppingCondition.wait(sleepTimeS)
        elif self._isStopping:
          return None
        else:
          self._moreMessagesOrStoppingCondition.wait()
      
  def pushWaiting(self, tmw: TelemessageWrapper):
    with self._lock:
      currentFirstWaitingMessage = self._waitingMessages[0] if len(self._waitingMessages) > 0 else None
      heappush(self._waitingMessages, tmw)
      if currentFirstWaitingMessage is None or tmw.scheduledDt < currentFirstWaitingMessage.scheduledDt:
        self._moreMessagesOrStoppingCondition.notify()

  def content(self):
    with self._lock:
      return self._waitingMessages + list(self._activeMessages.values())
    
  def stop(self):
    with self._lock:
      self._isStopping = True
      self._moreMessagesOrStoppingCondition.notifyAll()

class PooledTelemessageWriterDaemon(Thread):
  def __init__(self, queue: TelemessageWrapperQueue,
               url:str="https://neodata-ingress.eniris.be/v1/telemetry", params:dict[str, str]={}, authorizationHeaderFunction:Callable|None=None, timeoutS:float=60,
               session:requests.Session=None):
    super().__init__()
    self.daemon = True
    self.queue = queue
    self.url = url
    self.params = params
    self.authorizationHeaderFunction = authorizationHeaderFunction
    self.timeoutS = timeoutS
    self.session = requests.Session() if session is None else session

  def run(self):
    logging.debug("Started PooledTelemessageWriterDaemon")
    while True:
      print("getting")
      tmw = self.queue.popWaitingAndAddToActive()
      print("got", tmw)
      if tmw is None:
        logging.debug("Stopped PooledTelemessageWriterDaemon")
        return
      try:
        headers: dict[str, str] = {"Authorization": self.authorizationHeaderFunction()} if self.authorizationHeaderFunction is not None else {}
        resp = self.session.post(self.url, headers=headers, params=self.params|tmw.telemessage.parameters, timeout=self.timeoutS)
        if resp.status_code == 204:
          tmw.complete()
        elif resp.status_code in self.retryStatusCodes:
          tmw.reschedule(f"response with status code {resp.status_code} ({HTTPStatus(resp.status_code).phrase}): {resp.text}", self.queue)
        else:
          logging.exception(f"Dropping request due to response with status code {resp.status_code} ({HTTPStatus(resp.status_code).phrase}): {resp.text}")
      except requests.Timeout:
         tmw.reschedule("timeout", self.queue)

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
    self.telemessageWrapperQueue.stop()
    self.flush()