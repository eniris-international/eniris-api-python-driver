from threading import RLock
import time
from collections import OrderedDict
from typing import FrozenSet, Tuple

from eniris.point import Point
from eniris.point.writer.writer import PointWriterDecorator, PointWriter

NANOSECOND_CONVERSION = 10**9
SeriesKey = Tuple[FrozenSet[Tuple[str, str]], str, FrozenSet[Tuple[str, str]], str]


class PointDuplicateFilter(PointWriterDecorator):
    """The PointDuplicateFilter class is a PointWriter providing a thread-safe,
    in-memory storage for Points that have been processed before. It is designed to
    be a filter in the data pipeline, sitting between a source of Points and another
    "PointWriter" that it sends the filtered Points to.

    For every field of an incoming Point, it creates a hashable key to represent the
    series of the field. This key includes the Point's namespace, measurement, and tags.
    It checks if this exact field value (with the same series and timestamp) was
    processed recently. Fields which were recently processed will be passed to the list
    of outgoing Point's.

    The class removes old entries from its memory to keep its size in check, based on
    the maximum entry age and the maximum count of entries per series or in total.
    If the maximum number of entries grows too large, entries are removed according to
    a LRU (Least Recently Used) strategy.

    Args:
        maximumEntryAgeS (int, optional): The maximum time a field value will be stored\
          in the memory. The age of an entry is based on the time a field was last\
          submitted to the filter, and is unrelated to time timestamp of the field\
          itself. Defaults to two days: 2*24*3600
        maximumEntryCount (int, optional): The total maximum number of entries stored\
          in the memory of the filter. Defaults to 1_000
        maximumSeriesEntryCount (int, optional): The maximum number of entries stored\
          for a single series (i.e. namespace, measurement, tagset and field name\
          combination). Defaults to 10_000_000

    Example:
      >>> from eniris.point import Point
      >>> from eniris.point.writer import PointDuplicateFilter, PointPrinter
      >>> from datetime import datetime
      >>>
      >>> ns = {'database': 'myDatabase', 'retentionPolicy': 'myRetentionPolicy'}
      >>> pLiving0 = Point(ns, 'homeSensors', datetime(2023, 1, 1), {'id': 'livingroomSensor'}, {'temp_C': 18., 'humidity_perc': 20.})
      >>> pSauna0 = Point(ns, 'homeSensors', datetime(2023, 1, 1), {'id': 'saunaSensor'}, {'temp_C': 40., 'humidity_perc': 90.})
      >>> pLiving1 = Point(ns, 'homeSensors', datetime(2023, 1, 2), {'id': 'livingroomSensor'}, {'temp_C': 18., 'humidity_perc': 20.})
      >>>
      >>> writer = PointDuplicateFilter(PointPrinter(), maximumEntryAgeS=1, maximumSeriesEntryCount=2, maximumEntryCount=4)
      >>> writer.writePoints([pLiving0])
      PointPrinter [Point(namespace=V1Namespace(database='myDatabase', retentionPolicy='myRetentionPolicy'), measurement='homeSensors', time=datetime.datetime(2023, 1, 1, 0, 0), tags={'id': 'livingroomSensor'}, fields={'temp_C': 18.0, 'humidity_perc': 20.0})]
      >>> writer.writePoints([pLiving0, pSauna0])
      PointPrinter [Point(namespace=V1Namespace(database='myDatabase', retentionPolicy='myRetentionPolicy'), measurement='homeSensors', time=datetime.datetime(2023, 1, 1, 0, 0), tags={'id': 'saunaSensor'}, fields={'temp_C': 40.0, 'humidity_perc': 90.0})]
      >>> writer.writePoints([pLiving1])
      PointPrinter [Point(namespace=V1Namespace(database='myDatabase', retentionPolicy='myRetentionPolicy'), measurement='homeSensors', time=datetime.datetime(2023, 1, 2, 0, 0), tags={'id': 'livingroomSensor'}, fields={'temp_C': 18.0, 'humidity_perc': 20.0})]
    """

    def __init__(
        self,
        output: PointWriter,
        maximumEntryAgeS=2 * 24 * 3600,
        maximumSeriesEntryCount=1_000,
        maximumEntryCount=10_000_000,
    ):
        super().__init__(output)
        self.maximumEntryAgeS = maximumEntryAgeS
        self.maximumEntryCount = maximumEntryCount
        self.maximumSeriesEntryCount = maximumSeriesEntryCount
        self.memory: "dict[SeriesKey, OrderedDict[int, bool|int|float|str]]" = {}
        self.entryKey2updateTs: "OrderedDict[tuple[SeriesKey, int], int]" = (
            OrderedDict()
        )
        self.lock = RLock()

    def deleteExpiredEntries(self):
        """Remove all expired entries from the filter memory."""
        with self.lock:
            thresholdTimestamp = int(
                (time.time() - self.maximumEntryAgeS) * NANOSECOND_CONVERSION
            )
            while len(self.entryKey2updateTs) > 0:
                (pointDuplicateFilterMemoryKey, pointTimestamp) = next(
                    iter(self.entryKey2updateTs)
                )
                updateTimestamp = self.entryKey2updateTs[
                    (pointDuplicateFilterMemoryKey, pointTimestamp)
                ]
                if updateTimestamp > thresholdTimestamp:
                    break
                self._delete(pointDuplicateFilterMemoryKey, pointTimestamp)

    def _delete(self, pointSeriesKey: SeriesKey, pointTimestamp: int):
        """Remove a specific entry from the filter memory.
        This method is for internal use within the class, and should only be called for
        (pointSeriesKey, pointTimestamp) which exist, as it will otherwise throw
        an Exception.

        Args:
            pointSeriesKey (SeriesKey): The namespace, measurement and tag set of the\
              entry which must be removed
            pointTimestamp (int): The timestamp of the entry which must be removed
        """
        del self.entryKey2updateTs[(pointSeriesKey, pointTimestamp)]
        pointDuplicateFilterMemoryValue = self.memory[pointSeriesKey]
        del pointDuplicateFilterMemoryValue[pointTimestamp]
        if len(pointDuplicateFilterMemoryValue) == 0:
            del self.memory[pointSeriesKey]

    def writePoints(self, points: "list[Point]"):
        """Write points to the filter output, whilst filtering out field values which
        have been pushed before by looking if the new values are identical to the stored
        values.

        Args:
            points (list[eniris.point.Point]): List of Point's

        Returns:
            None: the filtered points are passed to the output of the filter
        """
        if len(points) == 0:
            return
        out: "list[Point]" = []
        with self.lock:
            self.deleteExpiredEntries()
            currentTs = int(time.time() * NANOSECOND_CONVERSION)
            for point in points:
                if point.time is None:
                    out.append(point)
                else:
                    pTs = int(point.time.timestamp() * NANOSECOND_CONVERSION)
                    pNamespaceParameters = point.namespace.toUrlParameters()
                    pNamespaceKey = frozenset(
                        (key, pNamespaceParameters[key]) for key in pNamespaceParameters
                    )
                    pTagsKey = frozenset((key, point.tags[key]) for key in point.tags)
                    updatedFields: "dict[str, bool|int|float|str]" = {}
                    for fieldKey in point.fields:
                        seriesKey = (
                            pNamespaceKey,
                            point.measurement,
                            pTagsKey,
                            fieldKey,
                        )
                        # Add an entry for the fields of p to the data structure
                        self.entryKey2updateTs[(seriesKey, pTs)] = currentTs
                        self.entryKey2updateTs.move_to_end((seriesKey, pTs))
                        cachedSeriesValues = self.memory.setdefault(
                            seriesKey, OrderedDict()
                        )
                        # Figure out whether the field was actually updated
                        fieldValue = point.fields[fieldKey]
                        if (
                            pTs not in cachedSeriesValues
                            or cachedSeriesValues[pTs] != fieldValue
                        ):
                            cachedSeriesValues[pTs] = fieldValue
                            updatedFields[fieldKey] = fieldValue
                        # Make sure the count constraints are not violated
                        while len(cachedSeriesValues) > self.maximumSeriesEntryCount:
                            p2Ts = next(iter(cachedSeriesValues))
                            self._delete(seriesKey, p2Ts)
                        while len(self.entryKey2updateTs) > self.maximumEntryCount:
                            (p2SeriesKey, p2Ts) = next(iter(self.entryKey2updateTs))
                            self._delete(p2SeriesKey, p2Ts)
                    # If necessary, add a point to the output list
                    if len(updatedFields) > 0:
                        out.append(
                            Point(
                                point.namespace,
                                point.measurement,
                                point.time,
                                point.tags,
                                updatedFields,
                            )
                        )
        if len(out) > 0:
            self.output.writePoints(out)
