from eniris.point import Point
from eniris.telemessage.writer import TelemessageWriter


class PointWriter:
    """PointWriters have a method writePoints which receives a list of Point's.
    Typically, they will perform some operations on these points and then either:
        - pass a modified list of Point's (i.e. PointWriterDecorator) to an output
        - pass a Telemessage (i.e. PointToTelemessageWriter) to an output
    """

    def writePoints(self, points: "list[Point]"):
        raise NotImplementedError("This method should be overridden in child classes")

    def flush(self):
        pass


class PointPrinter(PointWriter):
    """A PointWriter which prints any received points. Helpful for development
    and demonstration purposes"""

    def writePoints(self, points: "list[Point]"):
        print(f"PointPrinter {points}")


class PointWriterDecorator(PointWriter):
    """A base class for PointWriters which send their output to another PointWriter.
    This class should be inherited from"""

    def __init__(self, output: PointWriter):
        self.output = output

    def flush(self):
        """Flush any internal state, i.e. make sure that any internally stored or
        buffered points are transmitted and then flush the output"""
        self._flush()
        self.output.flush()

    def _flush(self):
        """A placeholder function which may be overridden in child classes. The
        overriding method only has to make sure that internally stored points are
        transmitted."""

    def writePoints(self, points: "list[Point]"):
        raise NotImplementedError("This method should be overridden in child classes")


class PointToTelemessageWriter(PointWriter):
    """A base class for PointWriters which send their output to a TelemessageWriter.
    This class should be inherited from"""

    def __init__(self, output: TelemessageWriter):
        self.output = output

    def flush(self):
        """Flush any internal state, i.e. make sure that any internally stored or
        buffered messages are transmitted and then flush the output"""
        self._flush()
        self.output.flush()

    def _flush(self):
        """A placeholder function which may be overridden in child classes. The
        overriding method only has to make sure that internally stored points are
        transmitted."""

    def writePoints(self, points: "list[Point]"):
        raise NotImplementedError("This method should be overridden in child classes")
