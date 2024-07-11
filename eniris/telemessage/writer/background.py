from typing import Callable, Optional
from datetime import datetime, timezone, timedelta
from threading import RLock, Thread, Event
from heapq import heappop, heappush
import logging
from http import HTTPStatus

import requests

from eniris.driver import DEFAULT_RETRY_CODES
from eniris.telemessage import Telemessage
from eniris.telemessage.writer.writer import TelemessageWriter


class TelemessageWrapper:

    def __init__(
        self,
        telemessage: Telemessage,
        scheduledDt: Optional[datetime] = None,
        retryNr: int = 0,
    ):
        self.telemessage = telemessage
        self._scheduledDt = (
            datetime.now(timezone.utc) if scheduledDt is None else scheduledDt
        )
        self._retryNr = retryNr

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
        self._has_new_messages = Event()
        if snapshotFolder is None:
            self._new_messages: "list[TelemessageWrapper]" = []
        else:
            # self._new_messages: "list[TelemessageWrapper]" = TelemessageWrapper.loadSnapshotsFromDirectory(snapshotFolder)
            self._new_messages: "list[TelemessageWrapper]" = []
            self._has_new_messages.set()
        self._pending_messages:"list[TelemessageWrapper]" = []      # This will function as our heap
        self._no_messages_left = Event()
        self._no_messages_left.set()
        
        self._worker_thread = Thread(target=self.__worker, daemon=True)
        self._worker_thread.start()
            

    def writeTelemessage(self, message: Telemessage):
        """
        Write a single telemetry message to the API
        """
        with self._lock:
            self._new_messages.append(TelemessageWrapper(message))
            self._has_new_messages.set()
            self._no_messages_left.clear()


    def flush(self):
        self._no_messages_left.wait()
    
         
    def __worker(self):
        while True:
            # Get the next message
            tmw = self.__get_next_tmw()
            # Try sending it to the database
            failure_reason, failed_tmw = self.__send_tmw(tmw)
            # Reschedule failed sends to a later moment
            if failed_tmw is not None:
                self.__reschedule(failure_reason, failed_tmw)
            # Signal if there are no more pending messages
            with self._lock:
                if (len(self._pending_messages) + len(self._new_messages)) == 0:
                    self._no_messages_left.set()
            
            
    def __send_tmw(self, tmw:TelemessageWrapper) -> "tuple[str, TelemessageWrapper]|tuple[None, None]":
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
            
            
    def __get_next_tmw(self) -> TelemessageWrapper:
        """
        Get the next TelemessageWrapper to send to the database.
        This function blocks until a) a new message arrives or b) the schedule time of the first pending message happens.
        We assume that new messages should are scheduled to be sent immediately (should be the case)
        """
        if len(self._pending_messages) == 0:
            wait_timeout_s = None
        else:
            wait_timeout_s = max(0.0, (self._pending_messages[0]._scheduledDt - datetime.now(timezone.utc)).total_seconds())
        # Wait until new messages arive or until the timeout has exceeded
        self._has_new_messages.wait(wait_timeout_s)
        # Put all new messages on the heap
        if self._has_new_messages.is_set():
            with self._lock:
                for tmw in self._new_messages:
                    heappush(self._pending_messages, tmw)
                self._new_messages = []
                self._has_new_messages.clear() 
        # Get the message that is scheduled to be send first
        return heappop(self._pending_messages)
    
    
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