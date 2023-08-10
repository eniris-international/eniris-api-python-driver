import logging
import time
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from threading import RLock, Thread, Condition
from typing import Union, Tuple, FrozenSet

from eniris.point import Point, Namespace, FieldSet
from eniris.point.writer.writer import PointToTelemessageWriter
from eniris.telemessage import Telemessage
from eniris.telemessage.writer import TelemessageWriter

# Constant to convert timestamps to nanoseconds
NANOSECOND_CONVERSION = 10**9

PointKey = Tuple[str, int, FrozenSet[Tuple[str, str]]]


def createPointKey(point: Point) -> PointKey:
    """Create a unique key for the given Point.

    Args:
        point (Point): Point for which to create the key.

    Returns:
        PointKey: A tuple containing the measurement, timestamp in nanoseconds,\
          and a frozenset of the tag key-value pairs.
    """
    return (
        point.measurement,
        int(point.time.timestamp() * NANOSECOND_CONVERSION),
        frozenset((tagKey, point.tags[tagKey]) for tagKey in point.tags),
    )


@dataclass
class PointBuffer:
    """A buffer containing points sharing a single namespace, allowing points to be
    appended and converted to Telemessages. The buffer keeps track of the total number
    of bytes of its contents when represented  in line protocol (including newline
    characters). Each PointBuffer also stores its creation datetime.

    This class is not internally thread-safe.
    """

    namespace: Namespace
    creationDt: datetime
    pointMap: "dict[PointKey, dict[str, Union[bool,float,int,str]]]"
    nrBytes: int

    def __init__(self, namespace: Namespace):
        self.namespace = namespace
        self.creationDt = datetime.now(timezone.utc)
        self.pointMap = {}
        self.nrBytes = 0

    def calculateNrExtraBytes(self, point: Point) -> int:
        """Calculate the change in the number of bytes of the buffer line protocol
        representation when a given Point would be added.

        Args:
        - point (Point): The point to evaluate.

        Returns:
        - int: The change in the number of bytes
        """
        nrExtraBytes = 0
        pointKey = createPointKey(point)
        if pointKey not in self.pointMap:
            nrExtraBytes += (
                len(Point.escapeMeasurement(point.measurement))
                + (1 + len(point.tags.toLineProtocol()) if len(point.tags) > 0 else 0)
                + (
                    1 + len(str(int(point.time.timestamp() * NANOSECOND_CONVERSION)))
                    if point.time is not None
                    else 0
                )
                + 1
            )
            existingFields = {}
        else:
            existingFields = self.pointMap[pointKey]
        newFields = point.fields
        for fieldKey in newFields:
            if fieldKey in existingFields:
                nrExtraBytes += len(FieldSet.escapeValue(newFields[fieldKey])) - len(
                    FieldSet.escapeValue(existingFields[fieldKey])
                )
            else:
                nrExtraBytes += (
                    1
                    + len(FieldSet.escapeKey(fieldKey))
                    + 1
                    + len(FieldSet.escapeValue(newFields[fieldKey]))
                )
        return nrExtraBytes

    def append(self, point: Point):
        """Append a point to the buffer. If a point with similar attributes exists,
        it will be updated.

        Args:
        - point (Point): The point to be appended.
        """
        self.nrBytes += self.calculateNrExtraBytes(point)
        pointKey = createPointKey(point)
        existingFields = self.pointMap.setdefault(pointKey, {})
        newFields = point.fields
        for fieldKey in newFields:
            existingFields[fieldKey] = newFields[fieldKey]

    def toPoints(self):
        """Convert the stored points in the buffer back to a list of Point objects.

        Returns:
        - list[Point]: A list of Point objects reconstructed from the buffer's contents.
        """
        return [
            Point(
                self.namespace,
                measurement,
                datetime.fromtimestamp(time / NANOSECOND_CONVERSION, tz=timezone.utc),
                {el[0]: el[1] for el in tagSet},
                self.pointMap[(measurement, time, tagSet)],
            )
            for (measurement, time, tagSet) in self.pointMap
        ]

    def toTelemessage(self):
        """Convert the stored points in the buffer into a Telemessage object.

        Returns:
        - Telemessage: A Telemessage representation of the points in the buffer.
        """
        return Telemessage(
            self.namespace.toUrlParameters(),
            [p.toLineProtocol().encode("utf-8") for p in self.toPoints()],
        )


class PointBufferDict:
    """A PointBufferDict manages a dictionary of PointBuffer instances,
    indexed by namespace. It ensures that each buffer stays within a defined size
    and handles flushing buffers to Telemessages.

    Args:
      maximumBatchSizeBytes (int, optional): Maximum size in bytes for internal\
        PointBuffer (i.e. each set of points sharing the same namespace).\
        Defaults to 10 MB
      maximumBufferSizeBytes (int, optional): Maximum size in bytes all buffers in the\
        entire dictionary combined. Defaults to 100 MB

    This class is thread-safe.
    """

    def __init__(
        self,
        maximumBatchSizeBytes: int = 10_000_000,
        maximumBufferSizeBytes: int = 100_000_000,
    ):
        self.maximumBatchSizeBytes = maximumBatchSizeBytes
        self.maximumBufferSizeBytes = maximumBufferSizeBytes
        self._lock = RLock()
        self._namespace2buffer: "dict[frozenset[tuple[str, str]], PointBuffer]" = {}
        self._nrBytes = 0
        self._hasNewContent: Condition = Condition(self._lock)

    def writePoints(self, points: "list[Point]"):
        """Writes a list of points to the buffer dictionary. If any buffer becomes too
        large, it will be flushed and a Telemessage object will be created.

        Args:
        - points (list[Point]): A list of points to write to the buffer.

        Returns:
        - list[Telemessage]: A list of Telemessage objects created from flushed buffers.
        """
        messages: "list[Telemessage]" = []
        if len(points) == 0:
            return messages
        with self._lock:
            # Add all points to namespace2buffer
            for point in points:
                namespaceParameters = point.namespace.toUrlParameters()
                namespaceKey = frozenset(
                    (key, namespaceParameters[key]) for key in namespaceParameters
                )
                buffer = self._namespace2buffer.setdefault(
                    namespaceKey, PointBuffer(point.namespace)
                )
                if (
                    buffer.nrBytes > 0
                    and buffer.nrBytes + buffer.calculateNrExtraBytes(point)
                    > self.maximumBatchSizeBytes
                ):
                    messages.append(buffer.toTelemessage())
                    self._nrBytes -= buffer.nrBytes
                    buffer = PointBuffer(point.namespace)
                    self._namespace2buffer[namespaceKey] = buffer
                # To keep self._nrBytes consistent, we remove the buffer's byte size,
                # add an element to the buffer, and then add the new buffer bytesize
                self._nrBytes -= buffer.nrBytes
                buffer.append(point)
                self._nrBytes += buffer.nrBytes
            # Check whether an immediate flush is required
            if self._nrBytes > self.maximumBufferSizeBytes:
                messages += self.flush()
            else:
                self._hasNewContent.notify()
        return messages

    def flush(self) -> "list[Telemessage]":
        """Flushes buffers from the dictionary, creating Telemessage objects
        for each set of points sharing the same namespace.
        This method will empty the buffer.

        Returns:
        - list[Telemessage]: A list of Telemessage objects created from the
          flushed buffers.
        """
        messages: "list[Telemessage]" = []
        with self._lock:
            for buffer in self._namespace2buffer.values():
                messages.append(buffer.toTelemessage())
            self._namespace2buffer = {}
            self._nrBytes = 0
        return messages


class BufferedPointToTelemessageWriter(PointToTelemessageWriter):
    """A PointWriter which outputs Telemessages asynchroneously.
    It combines messages sharing the same namespace in a buffer, outputting them in a
    single Telemessage, thus ensuring that Telemessages are written efficiently.
    The maximum size of individual buffers, the combined size of all buffers and the
    maximum time a point can be hold in a buffer before being written can be configured.
    This class is useful when one or more threads generate a large numer of small
    batches of Points.

    Args:
      lingerTimeS (float, optional): Maximum time a point can be hold in a buffer\
        before being written. Defaults to 1 s
      maximumBatchSizeBytes (int, optional): Maximum size in bytes for each PointBuffer\
        batch. Defaults to 10 MB
      maximumBufferSizeBytes (int, optional): Maximum size in bytes for the entire\
        buffer. Defaults to 100 MB

    Example:
      >>> from eniris.point import Point
      >>> from eniris.point.writer import BufferedPointToTelemessageWriter
      >>> from eniris.telemessage.writer import TelemessagePrinter
      >>> from datetime import datetime
      >>> import time
      >>>
      >>> ns = {'database': 'myDatabase', 'retentionPolicy': 'myRetentionPolicy'}
      >>> pLiving0 = Point(ns, 'homeSensors', datetime(2023, 1, 1), {'id': 'livingroomSensor'}, {'temp_C': 18., 'humidity_perc': 20.})
      >>> pSauna0 = Point(ns, 'homeSensors', datetime(2023, 1, 1), {'id': 'saunaSensor'}, {'temp_C': 40., 'humidity_perc': 90.})
      >>>
      >>> writer = BufferedPointToTelemessageWriter(TelemessagePrinter(), lingerTimeS=0.1)
      >>> writer.writePoints([pLiving0])
      >>> time.sleep(0.01)
      >>> writer.writePoints([pSauna0])
      >>> time.sleep(0.1)
      TelemessagePrinter Telemessage(parameters={'db': 'myDatabase', 'rp': 'myRetentionPolicy'}, lines=[b'homeSensors,id=livingroomSensor temp_C=18.0,humidity_perc=20.0 1672527600000000000', b'homeSensors,id=saunaSensor temp_C=40.0,humidity_perc=90.0 1672527600000000000'])
    """

    def __init__(
        self,
        output: TelemessageWriter,
        lingerTimeS: float = 1,
        maximumBatchSizeBytes: int = 10_000_000,
        maximumBufferSizeBytes: int = 100_000_000,
    ):
        super().__init__(output)
        self.pointBufferDict = PointBufferDict(
            maximumBatchSizeBytes, maximumBufferSizeBytes
        )
        self.daemon = BufferedPointToTelemessageWriterDaemon(
            output, self.pointBufferDict, lingerTimeS
        )
        self.daemon.start()

    def writePoints(self, points: "list[Point]"):
        """Write each Point of a list to its the buffer corresponding with its
        namespace. If a specific buffer becomes too large or if the combined size of
        all buffers becomed too large, it will be emptied and a Telemessage will be
        sent via the output.

        Args:
        - points (list[Point]): A list of points to write to the buffer.
        """
        for message in self.pointBufferDict.writePoints(points):
            try:
                self.output.writeTelemessage(message)
            except Exception: # pylint: disable=broad-exception-caught
                logging.exception(
                    "Failed to write Telemessage from "
                      +"BufferedPointToTelemessageWriter.writePoints"
                )

    def _flush(self):
        """Flushes all points from the namespace buffers, writing them to the output
        as Telemessages."""
        for message in self.pointBufferDict.flush():
            try:
                self.output.writeTelemessage(message)
            except Exception: # pylint: disable=broad-exception-caught
                logging.exception(
                    "Failed to write Telemessage from "
                      + "BufferedPointToTelemessageWriter.flush"
                )

    def __del__(self):
        """Destructor method for the BufferedPointToTelemessageWriter. Stops the
        daemon and flushes any remaining messages."""
        self.daemon.stop()
        self.flush()


class BufferedPointToTelemessageWriterDaemon(Thread):
    """Daemon thread for BufferedPointToTelemessageWriter, to ensure that points are
    outputted automatically when they are stored in a buffer for a specific duration.

    This daemon ensures that buffered points are written as Telemessages even if they
    don't fill a complete buffer by checking whether the buffer holding the point is
    older than a configured duration.

    Args:
      lingerTimeS (float, optional): Maximum time a point can be hold in a buffer\
        before being written. Defaults to 1 s
    """
    # pylint: disable=protected-access

    def __init__(
        self,
        output: TelemessageWriter,
        pointBufferDict: PointBufferDict,
        lingerTimeS: float = 1,
    ):
        super().__init__()
        self.lingerTimeS = lingerTimeS
        self.pointBufferDict = pointBufferDict
        self.daemon = True
        self._output = output
        self._isKilled: Condition = Condition(self.pointBufferDict._lock)

    def run(self) -> None:
        logging.debug("Started BufferedPointToTelemessageWriterDaemon")
        with self.pointBufferDict._lock:
            while True:
                while (
                    self._output is not None
                    and len(self.pointBufferDict._namespace2buffer) == 0
                ):
                    self.pointBufferDict._hasNewContent.wait()
                if self._output is None:
                    logging.debug("Stopped BufferedPointToTelemessageWriterDaemon")
                    return
                curDt = datetime.now(timezone.utc)
                # Empty the buffers with old content
                thresholdDt = curDt - timedelta(seconds=self.lingerTimeS)
                newNamespace2buffer = {}
                for key in self.pointBufferDict._namespace2buffer:
                    buffer = self.pointBufferDict._namespace2buffer[key]
                    if buffer.creationDt < thresholdDt:
                        try:
                            self._output.writeTelemessage(buffer.toTelemessage())
                        except Exception: # pylint: disable=broad-exception-caught
                            logging.exception(
                                "Failed to write Telemessage from "
                                  "BufferedPointToTelemessageWriterDaemon.run"
                            )
                        self.pointBufferDict._nrBytes -= buffer.nrBytes
                    else:
                        newNamespace2buffer[key] = buffer
                self.pointBufferDict._namespace2buffer = newNamespace2buffer
                # Check which buffer needs to be emptied next and sleep for an appropriate amount of time
                minCreationDt: "datetime|None" = None
                for buffer in self.pointBufferDict._namespace2buffer.values():
                    if minCreationDt is None or buffer.creationDt < minCreationDt:
                        minCreationDt = buffer.creationDt
                if minCreationDt is not None:
                    nextWakeupDt = minCreationDt + timedelta(seconds=self.lingerTimeS)
                    sleepTimeS = nextWakeupDt.timestamp() - time.time()
                    if sleepTimeS > 0:
                        self._isKilled.wait(sleepTimeS)

    def stop(self):
        """A method to stop the daemon thread."""
        with self.pointBufferDict._lock:
            self._output = None
            self._isKilled.notify()
