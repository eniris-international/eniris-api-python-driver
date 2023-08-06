import logging, time
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from threading import RLock, Thread, Condition
from typing import Union

from eniris.point import Point, Namespace, FieldSet
from eniris.point.writer import PointToTelemessageWriter
from eniris.telemessage import Telemessage
from eniris.telemessage.writer import TelemessageWriter

NANOSECOND_CONVERSION = 10**9

def createPointKey(point: Point):
    return (
        point.measurement,
        int(point.time.timestamp() * NANOSECOND_CONVERSION),
        frozenset((tagKey, point.tags[tagKey]) for tagKey in point.tags)
    )

@dataclass
class PointBuffer:
  namespace: Namespace
  creationDt: datetime
  pointMap: 'dict[tuple[str, int, frozenset[tuple[str, str]]], dict[str, Union[bool,float,int,str]]]'
  nrBytes: int

  def __init__(self, namespace: Union[Namespace,dict]):
    self.namespace = namespace
    self.creationDt = datetime.now(timezone.utc)
    self.pointMap = dict()
    self.nrBytes = 0
    
  def calculateNrExtraBytes(self, point: Point) -> int:
    nrExtraBytes = 0
    pointKey = createPointKey(point.measurement)
    if pointKey not in self.pointMap:
      nrExtraBytes += len(Point.escapeMeasurement(point.measurement)) + \
              (1 + len(point.tags.toLineProtocol()) if len(point.tags) > 0 else 0) + \
              (1 + len(str(int(point.time.timestamp()*NANOSECOND_CONVERSION))) if point.time is not None else 0) + \
              1
      existingFields = dict()
    else:
      existingFields = self.pointMap[pointKey]
    newFields = point.fields
    for fieldKey in newFields:
      if fieldKey in existingFields:
        nrExtraBytes += len(FieldSet.escapeValue(newFields[fieldKey])) - len(FieldSet.escapeValue(existingFields[fieldKey]))
      else:
        nrExtraBytes += 1 + len(FieldSet.escapeKey(fieldKey)) + 1 + len(FieldSet.escapeValue(newFields[fieldKey]))
    return nrExtraBytes

  def append(self, point: Point):
    self.nrBytes += self.calculateNrExtraBytes(point)
    pointKey = createPointKey(point)
    if pointKey not in self.pointMap:
      self.pointMap[pointKey] = dict()
    existingFields = self.pointMap[pointKey]
    newFields = point.fields
    for fieldKey in newFields:
      existingFields[fieldKey] = newFields[fieldKey]

  def toPoints(self):
    return [
      Point(
        self.namespace, measurement, datetime.fromtimestamp(time/NANOSECOND_CONVERSION, tz=timezone.utc),
        {el[0]: el[1] for el in tagSet}, self.pointMap[(measurement, time, tagSet)]
      ) for (measurement, time, tagSet) in self.pointMap
    ]
  
  def toTelemessage(self):
    return Telemessage(self.namespace.toUrlParameters(), [p.toLineProtocol().encode("utf-8") for p in self.toPoints()])
  
class PointBufferDict:
  def __init__(self,
        maximumBatchSizeBytes:int=1_000_000,
        maximumBufferSizeBytes:int=10_000_000):
    self.maximumBatchSizeBytes = maximumBatchSizeBytes
    self.maximumBufferSizeBytes = maximumBufferSizeBytes
    self._lock = RLock()
    self._namespace2buffer: 'dict[frozenset[tuple[str, str]], PointBuffer]' = dict()
    self._nrBytes = 0
    self._hasNewContent: Condition = Condition(self._lock)

  def writePoints(self, points: list[Point]):
    messages: list[Telemessage] = []
    if len(points) == 0:
      return messages
    with self._lock:
      # Add all points to namespace2buffer
      for point in points:
        namespace_parameters = point.namespace.toUrlParameters()
        namespace_key = frozenset((key, namespace_parameters[key]) for key in namespace_parameters)
        buffer = self._namespace2buffer.setdefault(namespace_key, PointBuffer(point.namespace))
        if buffer.nrBytes > 0 and buffer.nrBytes + buffer.calculateNrExtraBytes(point) > self.maximumBatchSizeBytes:
          messages.append(buffer.toTelemessage())
          self._nrBytes -= buffer.nrBytes
          buffer = PointBuffer(point.namespace)
          self._namespace2buffer[namespace_key] = buffer
        self._nrBytes -= buffer.nrBytes
        buffer.append(point)
        self._nrBytes += buffer.nrBytes
      # Check whether an immediate flush is required
      if self._nrBytes > self.maximumBufferSizeBytes:
        messages += self.flush()
      else:
        self._hasNewContent.notify()
    return messages
  
  def flush(self):
    messages: list[Telemessage] = []
    with self._lock:
      for buffer in self._namespace2buffer.values():
        messages.append(buffer.toTelemessage())
      self._namespace2buffer = dict()
      self._nrBytes = 0
    return messages

class BufferedPointToTelemessageWriter(PointToTelemessageWriter):
  def __init__(self, output: TelemessageWriter,
        lingerTimeS:float=0.1,
        maximumBatchSizeBytes:int=1_000_000,
        maximumBufferSizeBytes:int=10_000_000):
    super().__init__(output)
    self.pointBufferDict = PointBufferDict(maximumBatchSizeBytes, maximumBufferSizeBytes)
    self.daemon = BufferedPointToTelemessageWriterDaemon(output, self.pointBufferDict, lingerTimeS)
    self.daemon.start()

  def writePoints(self, points: list[Point]):
    for message in self.pointBufferDict.writePoints(points):
      try:
        self.output.writeTelemessage(message)
      except:
        logging.exception("Failed to write Telemessage from BufferedPointToTelemessageWriter.writePoints")
  
  def _flush(self):
    for message in self.pointBufferDict.flush():
      try:
        self.output.writeTelemessage(message)
      except:
        logging.exception("Failed to write Telemessage from BufferedPointToTelemessageWriter.flush")

  def __del__(self):
    self.daemon.stop()
    self.flush()

class BufferedPointToTelemessageWriterDaemon(Thread):
  def __init__(self, output: TelemessageWriter, pointBufferDict: PointBufferDict, lingerTimeS:float=0.1):
    super().__init__()
    self.lingerTimeS = lingerTimeS
    self.pointBufferDict = pointBufferDict
    self.daemon = True
    self._output = output
    self._isKilled: Condition = Condition(self.pointBufferDict._lock)

  def run(self):
    logging.debug("Started BufferedPointToTelemessageWriterDaemon")
    with self.pointBufferDict._lock:
      while True:
        while self._output is not None and len(self.pointBufferDict._namespace2buffer) == 0:
          self.pointBufferDict._hasNewContent.wait()
        if self._output is None:
          logging.debug("Stopped BufferedPointToTelemessageWriterDaemon")
          return
        curDt = datetime.now(timezone.utc)
        # Empty the buffers with old content
        thresholdDt = curDt - timedelta(seconds=self.lingerTimeS)
        newNamespace2buffer = dict()
        for key in self.pointBufferDict._namespace2buffer:
          buffer = self.pointBufferDict._namespace2buffer[key]
          if buffer.creationDt < thresholdDt:
            try:
              self._output.writeTelemessage(buffer.toTelemessage())
            except:
              logging.exception("Failed to write Telemessage from BufferedPointToTelemessageWriterDaemon.run")
            self.pointBufferDict._nrBytes -= buffer.nrBytes
          else:
            newNamespace2buffer[key] = buffer
        self.pointBufferDict._namespace2buffer = newNamespace2buffer
        # Check which buffer needs to be emptied next and sleep for an appropriate amount of time
        minCreationDt: datetime|None = None
        for buffer in self.pointBufferDict._namespace2buffer.values():
          if minCreationDt is None or buffer.creationDt<minCreationDt:
            minCreationDt = buffer.creationDt
        if minCreationDt is not None:
          nextWakeupDt = minCreationDt + timedelta(seconds=self.lingerTimeS)
          sleepTimeS = nextWakeupDt.timestamp()-time.time()
          if sleepTimeS>0:
            self._isKilled.wait(sleepTimeS)

  def stop(self):
    with self.pointBufferDict._lock:
      self._output = None
      self._isKilled.notify()
      self.pointBufferDict._hasNewContent.notify()