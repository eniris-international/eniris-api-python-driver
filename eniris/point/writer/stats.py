from dataclasses import asdict
from datetime import datetime, timezone

from eniris.point import Point
from eniris.point.namespace import Namespace
from eniris.point.writer.writer import PointWriterDecorator, PointWriter


class PointStatsPrinter(PointWriterDecorator):
    """
    PointStatsPrinter is a PointWriterDecorator that gathers statistics about the
    points written to a PointWriter. These statistics are printed, and the points
    are send on to the output PointWriter.
    
    This class is intended to be used during debugging, to localize where data
    originates.
    """

    def __init__(
        self,
        output: PointWriter,
        origin: str,
        enablePrint:bool = False,
        printNamespace: bool = True,
        printMeasurements: bool = True,
        printTags: bool = True,
        printFields: bool = True,
        printPoints: bool = False,
        loggingNamespace: "Namespace|None" = None,
        loggingMeasurement: "str|None" = None,
    ):
        super().__init__(output)
        self.enablePrint = enablePrint
        self.printNamespace = printNamespace
        self.printMeasurements = printMeasurements
        self.printTags = printTags
        self.printFields = printFields
        self.printPoints = printPoints
        self.origin = origin
        self.loggingNamespace = loggingNamespace
        self.loggingMeasurement = loggingMeasurement

    def printStats(self, points: "list[Point]"):
        """
        Print the statistics of the points.
        """
        if self.enablePrint:
            namespace:"dict[str, int]" = {}
            measurements:"dict[str, int]" = {}
            tags:"dict[str, int]" = {}
            fields:"dict[str, int]" = {}
            for point in points:
                if isinstance(point.namespace, Namespace):
                    _namespace = asdict(point.namespace)
                elif isinstance(point.namespace, dict):
                    _namespace = point.namespace
                else:
                    raise ValueError(f"Namespace of point {point} is not of the type dict or Namespace!")
                for value in _namespace.values():
                    if value not in namespace:
                        namespace[value] = 0
                    namespace[value] += 1
                if point.measurement not in measurements:
                    measurements[point.measurement] = 0
                measurements[point.measurement] += 1
                for value in point.tags.values():
                    if value not in tags:
                        tags[value] = 0
                    tags[value] += 1
                for key in point.fields:
                    if key not in fields:
                        fields[key] = 0
                    fields[key] += 1

            msg = "-----------------------------------------------------------------\n"
            msg += "Point statistics:\n"
            msg += "\n"
            msg += f"Origin: {self.origin}\n"
            msg += f"Total number of points: {len(points)}\n"
            msg += "\n"
            if self.printNamespace:
                msg += "Per namespace value:\n"
                for key, value in namespace.items():
                    msg += f"{key}: {value} points\n"
                msg += "\n"
            if self.printMeasurements:
                msg += "Per measurement:\n"
                for key, value in measurements.items():
                    msg += f"{key}: {value} points\n"
                msg += "\n"
            if self.printTags:
                msg += "Per tag value:\n"
                for key, value in tags.items():
                    msg += f"{key}: {value} points\n"
                msg += "\n"
            if self.printFields:
                msg += f"Per field (total number of field values written: {sum(fields.values())}):\n"
                for key, value in fields.items():
                    msg += f"{key}: {value} points\n"
                msg += "\n"
            if self.printPoints:
                msg += "Points:\n"
                for point in points:
                    msg += str(point.toJson())
                msg += "\n"
            msg += "-----------------------------------------------------------------"

            # Print as a single message, as an attempt to keep everything in the console together (in case of multithreaded
            # applications the log might otherwise get scattered)
            print(msg)

    def createTrackingPoints(self, points: "list[Point]") -> "list[Point]":
        """
        Create points that track the other points, amount of data etc.
        """
        out = []
        # There is also an extensively tagged system to easily track over time in Influx
        # where the points come from, what writes them, from where in the code
        if self.loggingNamespace is not None:
            analyzed:"dict[int,tuple[dict[str,str], int]]" = {}

            for point in points:
                if isinstance(point.namespace, Namespace):
                    _namespace = asdict(point.namespace)
                elif isinstance(point.namespace, dict):
                    _namespace = point.namespace
                else:
                    raise ValueError(f"Namespace of point {point} is not of the type dict or Namespace!")

                tags = {
                    'origin': self.origin,
                    **_namespace,
                    'measurement': point.measurement,
                    **point.tags,
                }

                def signature(tags:dict):
                    hash_ = 0
                    for pair in tags.items():
                        hash_ ^= hash(pair)
                    return hash_

                sig = signature(tags)
                if sig not in analyzed:
                    analyzed[sig] = (tags, 1)
                else:
                    analyzed[sig] = (tags, analyzed[sig][1]+1)

            for item in analyzed.values():
                tags = item[0]
                count = item[1]
                print(f'count: {count}. tags: {tags}')

                out.append(
                    Point(
                        self.loggingNamespace,
                        self.loggingMeasurement,
                        datetime.now(timezone.utc),
                        tags,
                        {"count": count},
                    )
                )
                
        return out

    def writePoints(self, points: "list[Point]"):
        """Write points to the filter output, whilst filtering out field values which
        have been pushed before by looking if the new values are identical to the stored
        values.

        Args:
            points (list[eniris.point.Point]): List of Points

        Returns:
            None: the filtered points are passed to the output of the filter
        """
        self.printStats(points)
        points.extend(self.createTrackingPoints(points))
        self.output.writePoints(points)
