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
    return (self.creationDt.isoformat(), self.subId)
  
  def hasSnapshot(self):
    with self._lock:
      return self._snapshotPath is not None
    
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
  
  def saveSnapshot(self, dirname:str):
    with self._lock:
      if self._isFinished:
        return
      try:
        snapshotPath = os.path.join(dirname, self.filename())
        with open(snapshotPath, 'w') as f:
          logging.debug(f"Writing Telemessage to '{snapshotPath}'")
          pickle.dump(self.telemessage, f)
        self._snapshotPath = snapshotPath
      except:
        logging.exception("Unable to store a snapshot due to an unexpected exception")

  def removeSnapshot(self):
    with self._lock:
      if not self._snapshotPath:
        return
      try:
        os.remove(self._snapshotPath)
        self._snapshotPath = None
      except:
        logging.exception("Unable to remove a snapshot due to an unexpected exception")

  def updateSnapshot(self):
    with self._lock:
      if self._snapshotPath:
        try:
          newSnapshotPath = os.path.join(os.path.dirname(self._snapshotPath), self.filename())
          os.rename(self._snapshotPath, newSnapshotPath)
          self._snapshotPath = newSnapshotPath
        except:
          logging.exception("Unable to rename a snapshot due to an unexpected exception")

  def reschedule(self, reason: str, queue):
    with self._lock:
      if self._retryNr+1 <= queue.maximumRetries:
        logging.warning(f"Retrying request after {reason}")
        self._scheduledDt = datetime.now(timezone.utc) + timedelta(seconds=min(queue.initialRetryDelayS*2**self._retryNr, queue.maximumRetryDelayS))
        self._retryNr += 1
        self.updateSnapshot()
      else:
        logging.error(f"Maximum number of retries exceeded, dropping telemessage due to {reason}")
        self.finish(queue)
        return
    queue.removeActive(self)
    queue.pushWaiting(self)

  def finish(self, queue):
    with self._lock:
      self._isFinished = True
      self._finishedCondition.notifyAll()
      self.removeSnapshot()
    queue.removeActive(self)

  def isFinished(self):
    with self._lock:
      return self._isFinished

  def wait(self, timeout:float|None=None):
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
    self._newMessageOrStoppingCondition: Condition = Condition(self._lock)
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
      self._newMessageOrStoppingCondition.notifyAll() # TODO: Only call this when the message is really real, and not when it is requeued
  
  def getContentOnNewMessage(self, latestKnownTmw: TelemessageWrapper=None):
    with self._lock:
      while True:
        content = self.content()
        for m in content:
          if (latestKnownTmw is None or (m.creationDt, m.subId) > (latestKnownTmw.creationDt, latestKnownTmw.subId)):
            return content
        if self._isStopping:
          return None
        else:
          self._newMessageOrStoppingCondition.wait()

  def content(self):
    with self._lock:
      return self._waitingMessages + list(self._activeMessages.values())
    
  def stop(self):
    with self._lock:
      self._isStopping = True
      self._moreMessagesOrStoppingCondition.notifyAll()
      self._newMessageOrStoppingCondition.notify_all()

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
      tmw = self.queue.popWaitingAndAddToActive()
      if tmw is None:
        logging.debug("Stopped PooledTelemessageWriterDaemon")
        return
      try:
        headers: dict[str, str] = {"Authorization": self.authorizationHeaderFunction()} if self.authorizationHeaderFunction is not None else {}
        resp = self.session.post(self.url, headers=headers, params=self.params|tmw.telemessage.parameters, timeout=self.timeoutS)
        if resp.status_code == 204:
          tmw.finish(self.queue)
        elif resp.status_code in self.retryStatusCodes:
          tmw.reschedule(f"response with status code {resp.status_code} ({HTTPStatus(resp.status_code).phrase}): {resp.text}", self.queue)
        else:
          logging.error(f"Dropping telemessage due to response with status code {resp.status_code} ({HTTPStatus(resp.status_code).phrase}): {resp.text}")
      except requests.Timeout:
         tmw.reschedule("timeout", self.queue)
      except:
        logging.exception("Dropping telemessage due to unexpected exception")
        tmw.finish(self.queue)

class PooledTelemessageSnapshotDaemon(Thread):
  def __init__(self, queue: TelemessageWrapperQueue,
               snapshotFolder:str, minimumSnaphotAgeS:int=5, maximumSnapshotStorageBytes=20_000_000):
    super().__init__()
    self.daemon = True
    self.queue = queue
    self.snapshotFolder = snapshotFolder
    self.minimumSnaphotAgeS = minimumSnaphotAgeS
    self.maximumSnapshotStorageBytes = maximumSnapshotStorageBytes

  def run(self):
    logging.debug("Started PooledTelemessageSnapshotDaemon")
    lastMessage = None
    nextMessageToBeSnapshotted = None
    while True:
      if nextMessageToBeSnapshotted is None:
        content = self.queue.getContentOnNewMessage(lastMessage)
      else:
        sleepTimeS = nextMessageToBeSnapshotted.creationDt.timestamp()+self.minimumSnaphotAgeS-time.time()
        if sleepTimeS>0:
          nextMessageToBeSnapshotted.wait(sleepTimeS)
        content = self.queue.content()
      if content is None:
        logging.debug("Stopped PooledTelemessageSnapshotDaemon")
        return
      content.sort(key=lambda x: x.id)
      currentlySnapshottedIds = {el.id for el in content if el.hasSnapshot()}
      thresholdDt = datetime.now()-timedelta(seconds=self.minimumSnaphotAgeS)
      desiredSnapshottedIds = set()
      usedBytes = 0
      for el in reversed(content):
        if el.creationDt < thresholdDt:
          if usedBytes + el.telemessage.nrBytes() < self.maximumSnapshotStorageBytes:
            desiredSnapshottedIds.add(el.id)
      for el in content:
        if el.id in currentlySnapshottedIds and el.id not in desiredSnapshottedIds:
          el.removeSnapshot()
      for el in content:
        if el.id not in currentlySnapshottedIds and el.id in desiredSnapshottedIds:
          el.saveSnapshot()
      nextMessageToBeSnapshotted = None
      for el in content:
        if el.creationDt >= thresholdDt:
          nextMessageToBeSnapshotted = el
          break
      if len(content) > 0:
        lastMessage = content[-1]

class PooledTelemessageWriter(TelemessageWriter):
  """Write telemessages (telemetry messages) to a specific endpoint in a blocking fashion:
  using this class to write messages will block a sending thread until the message is succesfully transmitted or an exception is raised.
  """
  def __init__(self, poolSize:int=1,
               snapshotFolder:str|None=None, minimumSnaphotAgeS:int=5, maximumSnapshotStorageBytes=20_000_000,
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
    self.queue = TelemessageWrapperQueue(maximumRetries=maximumRetries, initialRetryDelayS=initialRetryDelayS, maximumRetryDelayS=maximumRetryDelayS, retryStatusCodes=retryStatusCodes)
    session = requests.Session() if session is None else session
    self.pool = [PooledTelemessageWriterDaemon(self.queue,
      url=url, params=params, authorizationHeaderFunction=authorizationHeaderFunction, timeoutS=timeoutS,
      session=session) for i in range(poolSize)]
    for d in self.pool:
      d.start()
    if snapshotFolder is not None:
      self.snapshotDaemon = PooledTelemessageSnapshotDaemon(self.queue, snapshotFolder, minimumSnaphotAgeS, maximumSnapshotStorageBytes)
      self.snapshotDaemon.start()
  
  def writeTelemessage(self, tm: Telemessage):
    """
    Write a single telemetry message to the API
    """
    self.queue.pushWaiting(TelemessageWrapper(tm))

  def flush(self):
    for tmw in self.queue.content():
      tmw.wait()

  def __del__(self):
    self.queue.stop()
    self.flush()