from threading import RLock
import time
from collections import OrderedDict

from eniris.point import Point
from eniris.point.writer import PointWriterDecorator, PointWriter

NANOSECOND_CONVERSION = 10**9
PointDuplicateFilterMemoryKey = tuple[frozenset[tuple[str, str]], str, frozenset[tuple[str, str]]]

def createPointDuplicateFilterMemoryKey(point: Point) -> PointDuplicateFilterMemoryKey:
  namespace_parameters = point.namespace.toUrlParameters() 
  frozen_namespace = frozenset((key, namespace_parameters[key]) for key in namespace_parameters)
  frozen_tags = frozenset((key, point.tags[key]) for key in point.tags)
  return (frozen_namespace, point.measurement, frozen_tags)

class PointDuplicateFilter(PointWriterDecorator):
  def __init__(self, output: PointWriter, maximumEntryAgeS=2*24*3600, maximumSeriesEntryCount=500_000, maximumEntryCount=1_000_000_000):
    super().__init__(output)
    self.maximumEntryAgeS = maximumEntryAgeS
    self.maximumEntryCount = maximumEntryCount
    self.maximumSeriesEntryCount = maximumSeriesEntryCount
    self.memory: dict[PointDuplicateFilterMemoryKey, OrderedDict[int, dict[str, bool|int|float|str]]] = dict()
    self.entryKey2updateTs: OrderedDict[tuple[PointDuplicateFilterMemoryKey, int], int] = OrderedDict()
    self.lock = RLock()

  def deleteExpiredEntries(self):
    with self.lock:
      thresholdTimestamp = int((time.time()-self.maximumEntryAgeS)*NANOSECOND_CONVERSION)
      while len(self.entryKey2updateTs) > 0:
        (pointDuplicateFilterMemoryKey, pointTimestamp) = next(iter(self.entryKey2updateTs))
        updateTimestamp = self.entryKey2updateTs[(pointDuplicateFilterMemoryKey, pointTimestamp)]
        if updateTimestamp > thresholdTimestamp:
          break
        self._delete(pointDuplicateFilterMemoryKey, pointTimestamp)

  def _delete(self, pointDuplicateFilterMemoryKey: PointDuplicateFilterMemoryKey, pointTimestamp: int):
    del self.entryKey2updateTs[(pointDuplicateFilterMemoryKey, pointTimestamp)]
    pointDuplicateFilterMemoryValue = self.memory[pointDuplicateFilterMemoryKey]
    del pointDuplicateFilterMemoryValue[pointTimestamp]
    if len(pointDuplicateFilterMemoryValue) == 0:
      del self.memory[pointDuplicateFilterMemoryKey]

  def writePoints(self, points: 'list[Point]'):
    """Filter that filters out measurements that have been pushed before by looking if new measurements are identical to last pushed measurements.

    Args:
        measurements (list): List of measurements

    Returns:
        list: Remaining measurements after filtering
    """
    if len(points) == 0:
      return
    out: 'list[Point]' = []
    with self.lock:
      self.deleteExpiredEntries()
      currentTs = int(time.time()*NANOSECOND_CONVERSION)
      for p in points:
        pTs = int(p.time*NANOSECOND_CONVERSION) if p.time is not None else currentTs
        pMemoryKey = createPointDuplicateFilterMemoryKey(p)
        # Add an entry for the fields of p to the data structure
        self.entryKey2updateTs[(pMemoryKey, pTs)] = currentTs
        self.entryKey2updateTs.move_to_end((pMemoryKey, pTs))
        if pMemoryKey not in self.memory:
          self.memory[pMemoryKey] = OrderedDict()
        pMemoryValue = self.memory[pMemoryKey]
        if pTs not in pMemoryValue:
          pMemoryValue[pTs] = dict()
        memoryFields = pMemoryValue[pTs]
        # Make sure the count constraints are not violated
        while len(pMemoryValue) > self.maximumSeriesEntryCount:
          p2Ts = next(iter(pMemoryValue))
          self._delete(pMemoryKey, p2Ts)
        while len(self.entryKey2updateTs) > self.maximumEntryCount:
          (p2MemoryKey, p2Ts) = next(iter(self.entryKey2updateTs))
          self._delete(p2MemoryKey, p2Ts)
        # Figure out which fields were actually updated
        updatedFields = dict()
        for fieldKey in p.fields:
          fieldValue = p.fields[fieldKey]
          if fieldKey not in memoryFields or memoryFields[fieldKey] != fieldValue:
            memoryFields[fieldKey] = fieldValue
            updatedFields[fieldKey] = fieldValue
        # If necessary, add a point to the output list
        if len(updatedFields) > 0:
          out.append(Point(p.namespace, p.measurement, p.time, p.tags, updatedFields))
    if len(out) > 0:
      self.output.writePoints(out)