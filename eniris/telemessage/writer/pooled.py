from typing import Callable, ClassVar, Optional, Union
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from threading import Lock, RLock, Condition, Thread
from heapq import heappop, heappush
import os
import pickle
import re
import logging
import time
from http import HTTPStatus

import requests

from eniris.driver import DEFAULT_RETRY_CODES
from eniris.telemessage import Telemessage
from eniris.telemessage.writer.writer import TelemessageWriter


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

    def __init__(
        self,
        telemessage: Telemessage,
        creationDt: Optional[datetime] = None,
        subId: Optional[int] = None,
        scheduledDt: Optional[datetime] = None,
        retryNr: int = 0,
        snapshotPath: Optional[str] = None,
    ):
        self._lock = RLock()
        self.telemessage = telemessage
        self.creationDt = (
            datetime.now(timezone.utc) if creationDt is None else creationDt
        )
        self.subId = TelemessageWrapper.incrementSubId() if subId is None else subId
        self._scheduledDt = (
            datetime.now(timezone.utc) if scheduledDt is None else scheduledDt
        )
        self._retryNr = retryNr
        self._snapshotPath = snapshotPath
        self._finishedCondition = Condition(self._lock)
        self._isFinished = False

    @property
    def id(self):
        return (self.creationDt.isoformat(), self.subId)

    def _filename(self):
        return (
            f"{self.creationDt.strftime('%Y%m%dT%H%M%S%f')}_subId_{self.subId}"
            + f"_scheduledDt_{self._scheduledDt.strftime('%Y%m%dT%H%M%S%f')}"
            + f"_retryNr_{self._retryNr}.pickle"
        )

    def hasSnapshot(self):
        with self._lock:
            return self._snapshotPath is not None

    @staticmethod
    def loadSnapshotsFromDirectory(directory: str):
        snapshots: "list[TelemessageWrapper]" = []
        try:
            filenames = os.listdir(directory)
            for filename in filenames:
                snapshotPath = os.path.join(directory, filename)
                try:
                    telemessage = TelemessageWrapper.loadSnapshot(snapshotPath)
                    if telemessage is not None:
                        snapshots.append(telemessage)
                        logging.info(f"Loaded Telemessage from '{snapshotPath}'")
                except Exception:  # pylint: disable=broad-exception-caught
                    logging.exception(
                        f"Failed to load Telemessage from '{snapshotPath}'"
                    )
            logging.debug(f"Finished loading Telemessages from '{directory}'")
        except Exception:  # pylint: disable=broad-exception-caught
            logging.exception(f"Failed to load any Telemessages from '{directory}'")
        return snapshots

    @staticmethod
    def loadSnapshot(path: str):
        if not os.path.isfile(path):
            return None
        filename = os.path.basename(path)
        match = re.match(
            r"([^_]*)_subId_([^_]*)_scheduledDt_([^_]*)_retryNr_(\d+).pickle", filename
        )
        if not match:
            return None
        with open(path, "rb") as file:
            telemessage: Telemessage = pickle.load(file)
        creationDt = datetime.strptime(f"{match.group(1)}+00:00", "%Y%m%dT%H%M%S%f%z")
        subId = int(match.group(2))
        scheduledDt = datetime.strptime(f"{match.group(3)}+00:00", "%Y%m%dT%H%M%S%f%z")
        retryNr = int(match.group(4))
        return TelemessageWrapper(
            telemessage, creationDt, subId, scheduledDt, retryNr, path
        )

    def saveSnapshot(self, dirname: str):
        with self._lock:
            if self._isFinished:
                return
            try:
                snapshotPath = os.path.join(dirname, self._filename())
                with open(snapshotPath, "wb") as file:
                    pickle.dump(self.telemessage, file)
                logging.info(f"Saved Telemessage to '{snapshotPath}'")
                self._snapshotPath = snapshotPath
            except Exception:  # pylint: disable=broad-exception-caught
                logging.exception(f"Failed to save Telemessage to '{dirname}'")

    def removeSnapshot(self):
        with self._lock:
            if not self._snapshotPath:
                return
            try:
                os.remove(self._snapshotPath)
                logging.info(f"Removed Telemessage from '{self._snapshotPath}'")
                self._snapshotPath = None
            except Exception:  # pylint: disable=broad-exception-caught
                logging.exception(
                    f"Failed to remove a Telemessage from '{self._snapshotPath}'"
                )

    def updateSnapshot(self):
        with self._lock:
            if self._snapshotPath:
                newSnapshotPath = os.path.join(
                    os.path.dirname(self._snapshotPath), self._filename()
                )
                try:
                    os.rename(self._snapshotPath, newSnapshotPath)
                    logging.info(' '.join([
                        f"Moved Telemessage from '{self._snapshotPath}'",
                        f"to '{newSnapshotPath}'"
                    ]))
                    self._snapshotPath = newSnapshotPath
                except Exception:  # pylint: disable=broad-exception-caught
                    logging.exception(' '.join([
                        f"Failed to move Telemessage from '{self._snapshotPath}'",
                        f" to '{newSnapshotPath}'"
                    ]))

    def reschedule(self, reason: str, queue):
        with self._lock:
            if self._retryNr + 1 <= queue.maximumRetries:
                logging.warning(f"Retrying request after {reason}")
                self._scheduledDt = datetime.now(timezone.utc) + timedelta(
                    seconds=min(
                        queue.initialRetryDelayS * 2**self._retryNr,
                        queue.maximumRetryDelayS,
                    )
                )
                self._retryNr += 1
                self.updateSnapshot()
            else:
                logging.error(' '.join([
                    "Maximum number of retries exceeded,",
                    f"dropping telemessage due to {reason}"
                ]))
                self.finish(queue)
                return
        queue.addWaiting(self)

    def finish(self, queue):
        with self._lock:
            self._isFinished = True
            self._finishedCondition.notify_all()
            self.removeSnapshot()
        queue.removeActive(self)

    def isFinished(self):
        with self._lock:
            return self._isFinished

    def wait(self, timeout: "float|None" = None):
        with self._lock:
            if not self._isFinished:
                self._finishedCondition.wait(timeout)

    # Note, we assume that no other threads are trying to modify the
    # scheduledDt when comparing elements
    def __lt__(self, other):
        return self._scheduledDt < other._scheduledDt

    # Note, we assume that no other threads are trying to modify the
    # scheduledDt when comparing elements
    def __eq__(self, other):
        return self._scheduledDt == other._scheduledDt


class TelemessageWrapperQueue:
    def __init__(
        self,
        waitingMessages: "Optional[list[TelemessageWrapper]]" = None,
        maximumRetries: int = 4,
        initialRetryDelayS: int = 1,
        maximumRetryDelayS: int = 60,
    ):
        self.maximumRetries = maximumRetries
        self.initialRetryDelayS = initialRetryDelayS
        self.maximumRetryDelayS = maximumRetryDelayS
        self._lock = RLock()
        self._activeMessages: "dict[str, TelemessageWrapper]" = {}
        self._waitingMessages = [] if waitingMessages is None else list(waitingMessages)
        self._moreMessagesOrStoppingCondition: Condition = Condition(self._lock)
        self._newMessageOrStoppingCondition: Condition = Condition(self._lock)
        self._isStopping = False

    def removeActive(self, tmw: TelemessageWrapper):
        with self._lock:
            del self._activeMessages[tmw.id]

    def onWaitingToActive(self):
        with self._lock:
            while True:
                if len(self._waitingMessages) > 0:
                    tmw = self._waitingMessages[0]
                    # Since tmw._scheduledDt may only be changed while the wrapper is
                    # an active message, we may safely read the protected member
                    # _scheduledDt
                    sleepTimeS = tmw._scheduledDt.timestamp() - time.time()  # pylint: disable=protected-access
                    if sleepTimeS < 0:
                        self._moreMessagesOrStoppingCondition.notify()
                        heappop(self._waitingMessages)
                        self._activeMessages[tmw.id] = tmw
                        return tmw
                    self._moreMessagesOrStoppingCondition.wait(sleepTimeS)
                elif self._isStopping:
                    return None
                else:
                    self._moreMessagesOrStoppingCondition.wait()

    def addWaiting(self, tmw: TelemessageWrapper):
        with self._lock:
            if tmw.id in self._activeMessages:
                del self._activeMessages[tmw.id]
            else:
                self._newMessageOrStoppingCondition.notify_all()
            currentFirstWaitingMessage = (
                self._waitingMessages[0] if len(self._waitingMessages) > 0 else None
            )
            heappush(self._waitingMessages, tmw)
            # Since the protected member _scheduledDt may only be changed while
            # a wrapper is an active message, we may safely read it for both
            # tmw and currentFirstWaitingMessage
            if (
                currentFirstWaitingMessage is None
                or tmw._scheduledDt < currentFirstWaitingMessage._scheduledDt  # pylint: disable=protected-access
            ):
                self._moreMessagesOrStoppingCondition.notify()
    def getContentOnNewMessage(
        self, latestKnownTmw: Optional[TelemessageWrapper] = None
    ):
        with self._lock:
            while True:
                content = self.content()
                for message in content:
                    if latestKnownTmw is None or (message.creationDt, message.subId) > (
                        latestKnownTmw.creationDt,
                        latestKnownTmw.subId,
                    ):
                        return content
                if self._isStopping:
                    return None
                self._newMessageOrStoppingCondition.wait()

    def content(self):
        with self._lock:
            return self._waitingMessages + list(self._activeMessages.values())

    def stop(self):
        with self._lock:
            self._isStopping = True
            self._moreMessagesOrStoppingCondition.notify_all()
            self._newMessageOrStoppingCondition.notify_all()


class PooledTelemessageWriterDaemon(Thread):
    def __init__(
        self,
        queue: TelemessageWrapperQueue,
        url: str = "https://neodata-ingress.eniris.be/v1/telemetry",
        params: "Optional[dict[str, str]]" = None,
        authorizationHeaderFunction: "Union[Callable,None]" = None,
        timeoutS: float = 60,
        retryStatusCodes: "Optional[set[int|HTTPStatus]]" = None,
        session: Optional[requests.Session] = None,
    ):
        super().__init__()
        self.daemon = True
        self.queue = queue
        self.url = url
        self.params = params if params else {}
        self.authorizationHeaderFunction = authorizationHeaderFunction
        self.timeoutS = timeoutS
        self.retryStatusCodes: "set[int|HTTPStatus]" = (
            DEFAULT_RETRY_CODES
            if retryStatusCodes is None
            else retryStatusCodes
        )
        self.session = requests.Session() if session is None else session

    def run(self) -> None:
        logging.debug("Started PooledTelemessageWriterDaemon")
        while True:
            tmw = self.queue.onWaitingToActive()
            if tmw is None:
                logging.debug("Stopped PooledTelemessageWriterDaemon")
                return
            try:
                headers: "dict[str, str]" = (
                    {"Authorization": self.authorizationHeaderFunction()}
                    if self.authorizationHeaderFunction is not None
                    else {}
                )
                resp = self.session.post(
                    self.url,
                    params={**self.params, **tmw.telemessage.parameters},
                    data=b"\n".join(tmw.telemessage.lines),
                    headers=headers,
                    timeout=self.timeoutS,
                )
                if resp.status_code == 204:
                    tmw.finish(self.queue)
                elif resp.status_code in self.retryStatusCodes:
                    tmw.reschedule(
                        f"response with status code {resp.status_code} "
                        + f"({HTTPStatus(resp.status_code).phrase}): {resp.text}",
                        self.queue,
                    )
                else:
                    logging.error(
                        " ".join([
                            "Dropping telemessage due to",
                            f"response with status code {resp.status_code}",
                            f"({HTTPStatus(resp.status_code).phrase}): {resp.text}"
                        ])
                    )
            except requests.Timeout:
                tmw.reschedule("timeout", self.queue)
            except requests.ConnectionError:
                tmw.reschedule("connection error", self.queue)
            except Exception:  # pylint: disable=broad-exception-caught
                logging.exception("Dropping telemessage due to unexpected exception")
                tmw.finish(self.queue)


class PooledTelemessageSnapshotDaemon(Thread):
    def __init__(
        self,
        queue: TelemessageWrapperQueue,
        snapshotFolder: str,
        minimumSnaphotAgeS: int = 5,
        maximumSnapshotStorageBytes=20_000_000,
    ):
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
                sleepTimeS = (
                    nextMessageToBeSnapshotted.creationDt.timestamp()
                    + self.minimumSnaphotAgeS
                    - time.time()
                )
                if sleepTimeS > 0:
                    nextMessageToBeSnapshotted.wait(sleepTimeS)
                content = self.queue.content()
            if content is None:
                logging.debug("Stopped PooledTelemessageSnapshotDaemon")
                return
            content.sort(key=lambda x: x.id)
            nextMessageToBeSnapshotted = self.fixSnaphots(content)
            if len(content) > 0:
                lastMessage = content[-1]

    def fixSnaphots(self, content: "list[TelemessageWrapper]", useAgeLimit=True):
        content.sort(key=lambda x: x.id)
        currentlySnapshottedIds = {el.id for el in content if el.hasSnapshot()}
        thresholdDt = datetime.now(timezone.utc) - timedelta(
            seconds=self.minimumSnaphotAgeS
        )
        desiredSnapshottedIds = set()
        usedBytes = 0
        for wrapper in reversed(content):
            if not useAgeLimit or wrapper.creationDt < thresholdDt:
                if (
                    usedBytes + wrapper.telemessage.nrBytes()
                    < self.maximumSnapshotStorageBytes
                ):
                    desiredSnapshottedIds.add(wrapper.id)
        for wrapper in content:
            if (
                wrapper.id in currentlySnapshottedIds
                and wrapper.id not in desiredSnapshottedIds
            ):
                wrapper.removeSnapshot()
        for wrapper in content:
            if (
                wrapper.id not in currentlySnapshottedIds
                and wrapper.id in desiredSnapshottedIds
            ):
                wrapper.saveSnapshot(self.snapshotFolder)
        for wrapper in content:
            if wrapper.creationDt >= thresholdDt:
                return wrapper
        return None


class PooledTelemessageWriter(TelemessageWriter):
    """Write telemessages (telemetry messages) to a specific endpoint
    in a non-blocking fashion: using this class to write messages will
    add the message to a LIFO (last-in-first-out) queue.
    This queue is server by a pool of worker threads, which also take
    care of message retries. To avoid dataloss in case of unexpected shutdowns,
    Messages which remain unsent can be snapshotted a specific folder.

    Args:
        url (str, optional): The url to which the Telemessages will be posted.\
            Defaults to https://neodata-ingress.eniris.be/v1/telemetry
        params (dict[str, str], optional): A dictionary with fixed parameters which\
            should be included in each request. Defaults to an empty dictionary
        authorizationHeaderFunction (Callable|None, optional): A function returning\
            a valid authorization header, if None no authorization header is\
            attached to the request. Defaults to None
        timeoutS (int, optional): Request timeout in seconds. Defaults to 60
        session (requests.Session, optional): A session object to use for all
            calls. If None, a requests.Session without extra options is created.\
            Defaults to None
        maximumRetries (int, optional): How many times to try again in case of a\
            failure. Defaults to 4
        initialRetryDelayS (int, optional): The initial delay between successive\
            retries in seconds. Defaults to 1
        maximumRetryDelayS (int, optional): The maximum delay between successive\
            retries in seconds. Defaults to 60
        retryStatusCodes (set[int], optional): A set of all response code for which\
            a retry attempt must be made. Defaults to {429, 500, 503}
    """

    def __init__(
        self,
        poolSize: int = 1,
        snapshotFolder: "str|None" = None,
        minimumSnaphotAgeS: int = 5,
        maximumSnapshotStorageBytes=20_000_000,
        url: str = "https://neodata-ingress.eniris.be/v1/telemetry",
        params: "Optional[dict[str, str]]" = None,
        authorizationHeaderFunction: "Callable|None" = None,
        timeoutS: float = 60,
        session: Optional[requests.Session] = None,
        maximumRetries: int = 4,
        initialRetryDelayS: int = 1,
        maximumRetryDelayS: int = 60,
        retryStatusCodes: "Optional[set[int|HTTPStatus]]" = None,
    ):
        params = {} if params is None else params
        retryStatusCodes = (
            DEFAULT_RETRY_CODES
            if retryStatusCodes is None
            else retryStatusCodes
        )
        waitingMessages: "list[TelemessageWrapper]" = (
            []
            if snapshotFolder is None
            else TelemessageWrapper.loadSnapshotsFromDirectory(snapshotFolder)
        )
        self.queue = TelemessageWrapperQueue(
            waitingMessages,
            maximumRetries=maximumRetries,
            initialRetryDelayS=initialRetryDelayS,
            maximumRetryDelayS=maximumRetryDelayS,
        )
        session = requests.Session() if session is None else session
        self.pool = [
            PooledTelemessageWriterDaemon(
                self.queue,
                url=url,
                params=params,
                authorizationHeaderFunction=authorizationHeaderFunction,
                timeoutS=timeoutS,
                retryStatusCodes=retryStatusCodes,
                session=session,
            )
            for i in range(poolSize)
        ]
        for deamon in self.pool:
            deamon.start()
        self.snapshotDaemon: "PooledTelemessageSnapshotDaemon|None" = None
        if snapshotFolder is not None:
            self.snapshotDaemon = PooledTelemessageSnapshotDaemon(
                self.queue,
                snapshotFolder,
                minimumSnaphotAgeS,
                maximumSnapshotStorageBytes,
            )
            self.snapshotDaemon.start()

    def writeTelemessage(self, message: Telemessage):
        """
        Write a single telemetry message to the API
        """
        self.queue.addWaiting(TelemessageWrapper(message))

    def flush(self):
        for tmw in self.queue.content():
            tmw.wait()

    def __del__(self):
        self.queue.stop()
        for deamon in self.pool:
            deamon.join()
        if self.snapshotDaemon is not None:
            self.snapshotDaemon.join()
            # self.snapshotDaemon.fixSnaphots(self.queue.content(), False)
