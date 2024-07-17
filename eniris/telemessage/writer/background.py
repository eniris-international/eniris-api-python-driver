from typing import Callable, ClassVar, Optional, Union
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from threading import Lock, RLock, Thread, Event
from heapq import heappop, heappush
import logging
from http import HTTPStatus
import os
import re
import pickle
from time import time

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
    ):
        self.telemessage = telemessage
        self.creationDt = (
            datetime.now(timezone.utc) if creationDt is None else creationDt
        )
        self._scheduledDt = self.creationDt
        self.subId = TelemessageWrapper.incrementSubId() if subId is None else subId
        self._retryNr = 0

    # Note, we assume that no other threads are trying to modify the
    # scheduledDt when comparing elements
    def __lt__(self, other):
        return self._scheduledDt < other._scheduledDt

    # Note, we assume that no other threads are trying to modify the
    # scheduledDt when comparing elements
    def __eq__(self, other):
        return self._scheduledDt == other._scheduledDt
    

class BackgroundTelemessageWriter(TelemessageWriter):
    """Write telemessages (telemetry messages) to a specific endpoint
    in a non-blocking fashion: using this class to write messages will
    add the message to a LIFO (last-in-first-out) queue.
    This queue is server by a signle worker thread, which also take
    care of message retries. To avoid dataloss in case of unexpected shutdowns,
    Messages which remain unsent can be snapshotted a specific folder.
    The snapshots will be reloaded when the service starts again.
    
    If there is a snapshotFolder and the writer is closed or destroyed, then
    all pending messages will be snapshotted at closure. If there isn't, all
    messages will be written immediately, and dropped at a failure.

    Args:
        snapshotFolder (str, optional): Folder where snapshots will be stored.\
            This must be unique for this instance! Don't let to instances write
            to the same folder.
        minimumSnaphotAgeS (float, optional): Minimum time telemessages must be\
            in memory before they are snapshotted. Defaults to 60.0 seconds.
        snapshotPeriodS (float, optional): How often the snapshot service will\
            take snapshots. If the service is shut down, this is the maximum 
            data loss period. Defaults to 3600.0 seconds.
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
        maxHeapSize (int, optional): Maximum amount of items that may be on the heap.
    """

    def __init__(
        self,
        snapshotFolder: "str|None" = None,
        minimumSnaphotAgeS: float = 60.0,
        snapshotPeriodS: float = 3600.0,
        url: str = "https://neodata-ingress.eniris.be/v1/telemetry",
        params: "Optional[dict[str, str]]" = None,
        authorizationHeaderFunction: "Callable|None" = None,
        timeoutS: float = 60,
        session: Optional[requests.Session] = None,
        maximumRetries: int = 4,
        initialRetryDelayS: int = 1,
        maximumRetryDelayS: int = 60,
        retryStatusCodes: "Optional[set[int|HTTPStatus]]" = None,
        maxHeapSize: int = None,
        **kwargs
    ):
        self.daemon = BackgroundTelemessageWriterDaemon(
                snapshotFolder,
                minimumSnaphotAgeS,
                snapshotPeriodS,
                url,
                params,
                authorizationHeaderFunction,
                timeoutS,
                session,
                maximumRetries,
                initialRetryDelayS,
                maximumRetryDelayS,
                retryStatusCodes,
                maxHeapSize,
                **kwargs
            )
        self.daemon.start()
            

    def writeTelemessage(self, message: Telemessage):
        """
        Write a single telemetry message to the API
        """
        self.daemon.writeTelemessage(message)
        
    def close(self):
        self.daemon.close()
        self.daemon.join()
        
        
    def __del__(self):
        self.close()
            
            
class BackgroundTelemessageWriterDaemon(Thread):
    
    def __init__(
        self,
        snapshotFolder: "str|None" = None,
        minimumSnaphotAgeS: float = 60.0,
        snapshotPeriodS: float = 3600.0,
        url: str = "https://neodata-ingress.eniris.be/v1/telemetry",
        params: "Optional[dict[str, str]]" = None,
        authorizationHeaderFunction: "Callable|None" = None,
        timeoutS: float = 60,
        session: Optional[requests.Session] = None,
        maximumRetries: int = 4,
        initialRetryDelayS: int = 1,
        maximumRetryDelayS: int = 60,
        retryStatusCodes: "Optional[set[int|HTTPStatus]]" = None,
        maxHeapSize: int = None,
        **kwargs
    ):
        super().__init__(
            name="bg-telemessage-writer"
        )
        
        self.url = url
        self.authorizationHeaderFunction = authorizationHeaderFunction
        self.session = requests.Session() if session is None else session
        self.timeoutS = timeoutS
        self.params = {} if params is None else params
        self.retryStatusCodes: "set[int|HTTPStatus]" = (
            DEFAULT_RETRY_CODES if retryStatusCodes is None else retryStatusCodes
        )
        
        self.maximumRetries = maximumRetries
        self.initialRetryDelayS = initialRetryDelayS
        self.maximumRetryDelayS = maximumRetryDelayS
        
        self._lock = RLock()
        self._stop_signal = Event()
        self._has_new_messages_or_stop = Event()        # At a stop, both self._stop and self._has_new_messages_or_stop are set!
        self._max_heap_size = maxHeapSize
        
        self._snapshot_folder = snapshotFolder                
        self._min_snapshot_age_s = minimumSnaphotAgeS
        self._snapshot_period_s = snapshotPeriodS
        self._next_snapshot_moment = time() + self._snapshot_period_s
        if snapshotFolder is None:
            self._new_messages: "list[TelemessageWrapper]" = []
        else:
            self._new_messages: "list[TelemessageWrapper]" = self.__loadSnapshots(snapshotFolder)
            if len(self._new_messages) > 0:
                self._has_new_messages_or_stop.set()
            
        self._pending_messages:"list[TelemessageWrapper]" = []      # This will function as our heap
        self._no_messages_left = Event()
        self._no_messages_left.set()
        
        
            
    def writeTelemessage(self, message: Telemessage):
        """
        Write a single telemetry message to the API
        """
        with self._lock:
            self._new_messages.append(TelemessageWrapper(message))
            self._has_new_messages_or_stop.set()
            self._no_messages_left.clear()


    def flush(self):
        """Wait until there are no pending messages left"""
        self._no_messages_left.wait()
        
        
    def close(self, blocking:bool=False):
        """
        Closes the BackgroundTelemessageWriter.
        Pending messages will be flushed.
        After closing the BackgroundTelemessageWriter will no longer send messages.
        """
        self._stop_signal.set()        # Set the stop event first!
        self._has_new_messages_or_stop.set()
        if blocking:
            self._no_messages_left.wait()
        
    
    def stop(self):
        """A method to stop the daemon thread."""
        self.close()
        
        
    def __del__(self):
        self.close()
   
        
    def __loadSnapshots(self, snapshot_folder:str) -> "list[TelemessageWrapper]":
        if not os.path.exists(snapshot_folder):
            return []
        try:
            existing_snapshot_filenames = set(os.listdir(snapshot_folder))
        except Exception as e:
            logging.error(f"Failed reading content of snapshot folder {snapshot_folder}!")
            return []
        
        result:"list[TelemessageWrapper]" = []
        for filename in existing_snapshot_filenames:
            match = re.match(
                r"([^_]*)_subId_([^_]*).pickle", filename
            )
            if not match:
                continue
            snapshotPath = os.path.join(snapshot_folder, filename)
            try:
                with open(snapshotPath, "rb") as file:
                    telemessage: Telemessage = pickle.load(file)
                creationDt = datetime.strptime(f"{match.group(1)}+00:00", "%Y%m%dT%H%M%S%f%z")
                subId = int(match.group(2))
                result.append(TelemessageWrapper(telemessage, creationDt, subId))
            except Exception as e:
                logging.error(f"Failed loading snapshot {snapshotPath}. Exception: {e}")
        return result
    
         
    def run(self):
        while True:
            # Get the next message
            tmw = self.__get_next()
            # tmw will be None if everything has been send and there is a "stop" signal.
            # So write the pending messages to disk and end the worker
            if tmw is None:
                self.__take_snapshot()
                return
            # Try sending it to the database
            failure_reason, failed_tmw = self.__send(tmw)
            # Reschedule failed sends to a later moment
            if failed_tmw is not None:
                self.__reschedule(failure_reason, failed_tmw)
            # Make sure the heap doesn't become too big
            self.__lazy_limit_heap_size()
            # Create snapshots if required
            if time() > self._next_snapshot_moment:
                self.__take_snapshot()
                self._next_snapshot_moment = time() + self._snapshot_period_s
            # Signal if there are no more pending messages
            with self._lock:
                if (len(self._pending_messages) + len(self._new_messages)) == 0:
                    self._no_messages_left.set()
            
            
    def __send(self, tmw:TelemessageWrapper) -> "tuple[str, TelemessageWrapper]|tuple[None, None]":
        """
        Send the Telemessage to the database.
        If the sending fails, then the reason and the telemessage wrapper is returned.
        Otherwise None, None is returned.
        """
        try:
            headers: "dict[str, str]" = (
                {"Authorization": self.authorizationHeaderFunction()}
                if self.authorizationHeaderFunction is not None
                else {}
            )
            resp = self.session.post(
                self.url,
                params={**self.params, **tmw.telemessage.parameters},
                data=tmw.telemessage.data,
                headers={**headers, **tmw.telemessage.headers},
                timeout=self.timeoutS,
            )
            if resp.status_code == 204:
                return None, None
            elif resp.status_code in self.retryStatusCodes:
                return (
                    f"response with status code {resp.status_code} "
                    + f"({HTTPStatus(resp.status_code).phrase}): {resp.text}",
                    tmw,
                )
            else:
                logging.error(
                    " ".join(
                        [
                            "Dropping telemessage due to",
                            f"response with status code {resp.status_code}",
                            f"({HTTPStatus(resp.status_code).phrase}): {resp.text}.",
                            f"Request telemessage data: {tmw.telemessage.data}",
                        ]
                    )
                )
                return None, None
        except requests.Timeout:
            return ("timeout", tmw)
        except requests.ConnectionError:
            return ("connection error", tmw)
        except Exception:  # pylint: disable=broad-exception-caught
            logging.exception("Dropping telemessage due to unexpected exception")
            return None, None
            
            
    def __get_next(self) -> "TelemessageWrapper|None":
        """
        Get the next TelemessageWrapper to send to the database.
        This function blocks until a) a new message arrives or b) the schedule time of the first pending message happens.
        We assume that new messages are scheduled to be sent immediately (should be the case!), so they'll always bubble
        to the top of the heap immediately.
        
        This function returns the next TelemessageWrapper to be sent, except when a stop signal is received. In that case 
        it returns None.
        """
        if len(self._pending_messages) == 0:
            # If there are no more messages pending, then wait until new messages arrive
            wait_timeout_s = None
        else:
            # Else wait at most until the next pending message is scheduled to be send
            wait_timeout_s = max(0.0, (self._pending_messages[0]._scheduledDt - datetime.now(timezone.utc)).total_seconds())
        while True:
            
            if not self._stop_signal.is_set():
                # Wait until new messages arive or until the timeout has exceeded
                # - but only if there is no stop event
                self._has_new_messages_or_stop.wait(wait_timeout_s)
                
            # Put all new messages (if any) on the heap
            with self._lock:
                for tmw in self._new_messages:
                    heappush(self._pending_messages, tmw)
                self._new_messages = []
                self._has_new_messages_or_stop.clear()
                
            # If a stop event is set:
            if self._stop_signal.is_set():
                if self._snapshot_folder is None:
                    # In case of a stop event when there is no snapshot folder, try writing each pending message exactly once.
                    if len(self._pending_messages) == 0:
                        return None
                    tmw = heappop(self._pending_messages)
                    tmw._retryNr = self.maximumRetries      # This forces that there will only be one try, no reschedule.
                    return tmw
                else:
                    # Else returning None signals to snapshot all pending messages
                    return None
            elif len(self._pending_messages) > 0:
                # if not self._stop.is_set(), then there should always be pending messages - but better safe than sorry so check!
                # Get the message that is scheduled to be send first
                return heappop(self._pending_messages)
            else:
                # There are no pending messages, so wait until new messages arrive
                wait_timeout_s = None
    
    
    def __reschedule(self, reason:str, tmw:TelemessageWrapper):
        """ Reschedule sending the telemessage to a later moment - if possible - otherwise it is dropped. """
        if tmw._retryNr + 1 <= self.maximumRetries:
            logging.warning(f"Retrying request after {reason}")
            self._scheduledDt = datetime.now(timezone.utc) + timedelta(
                seconds=min(
                    self.initialRetryDelayS * 2**tmw._retryNr,
                    self.maximumRetryDelayS,
                )
            )
            tmw._retryNr += 1
            heappush(self._pending_messages, tmw)
        else:
            logging.error(
                " ".join(
                    [
                        "Maximum number of retries exceeded,",
                        f"dropping telemessage due to {reason}",
                    ]
                )
            )
            
            
    def __lazy_limit_heap_size(self):
        """ Limit the amount of messages that still has to be send. """
        # We know from the docs of heapq that:
        # "Heaps are arrays for which a[k] <= a[2*k+1] and a[k] <= a[2*k+2] for all k, counting elements from 0"
        # There is no guarantee that this is perfectly sorted from early to late in our case, but at least messages
        # at the end of the queue will tend to be later. Our lazy approach to limit the size of the heap is to drop
        # messages that are at the end of the heap. This is fast and easy.
        if self._max_heap_size is None:
            return
        while len(self._pending_messages) > self._max_heap_size:
            self._pending_messages.pop()
            
            
    def __take_snapshot(self):
        """
        Take a snapshot of all the telemessages that are currently loaded in memory, so that they are preserved
        in case the application restarts. Only telemessages that are sufficiently old are snapshotted.
        If a telemessage doesn't exist any more in memory (either it has been written to the database or it has been discarted),
        then its snapshot is also removed.
        """
        if self._snapshot_folder is None:
            return
        
        # Make sure the snapshot folder exists
        try:
            os.makedirs(self._snapshot_folder, exist_ok=True)
        except Exception as e:
            logging.error(f"Failed creating snapshot folder {self._snapshot_folder}! Not taking snapshots")
            return
        
        # Check which snapshots exist
        existing_snapshot_filenames = set(os.listdir(self._snapshot_folder))
        used_snapshot_filenames = set()
        
        # Write all telemessages to the snapshot folder if they haven't been written yet.
        dt_limit = datetime.now(timezone.utc) - timedelta(seconds=self._min_snapshot_age_s)
        
        for tmw in self._pending_messages:
            if tmw.creationDt > dt_limit:
                continue
            filename = f"{tmw.creationDt.strftime('%Y%m%dT%H%M%S%f')}_subId_{tmw.subId}.pickle"
            used_snapshot_filenames.add(filename)
            # Don't snapshot messages that are already snapshotted
            if filename in existing_snapshot_filenames:
                continue
            snapshotPath = os.path.join(self._snapshot_folder, filename)
            try:
                with open(snapshotPath, "wb") as file:
                    pickle.dump(tmw.telemessage, file)
                logging.info(f"Saved Telemessage to '{snapshotPath}'")
            except Exception as e:
                logging.error(f"Error while saving Telemessage to '{snapshotPath}'!"
                              f"Exception: {e}")
            except:
                logging.error(f"Error while saving Telemessage to '{snapshotPath}'!"
                              "Check file permissions and the existence of the snapshot folder.")
            
        # Remove all telemessages from the snapshot folder that are no longer in memory.
        obsolete_snapshot_filenames = existing_snapshot_filenames.difference(used_snapshot_filenames)
        for filename in obsolete_snapshot_filenames:
            snapshotPath = os.path.join(self._snapshot_folder, filename)
            try:
                os.remove(snapshotPath)
            except Exception as e:
                logging.error(f"Error while removing snapshot {snapshotPath}. Exception: {e}")
            except:
                logging.error(f"Unknown error while removing snapshot {snapshotPath}.")
                