from eniris.point import Point
from eniris.telemessage.writer import TelemessageWriter

class PointWriter:
  def writePoints(self, points: 'list[Point]'):
    raise NotImplementedError("This method should be overrriden in child classes")
  
  def flush(self):
    pass
  
class PointPrinter(PointWriter):
  def writePoints(self, points: 'list[Point]'):
    print("PointPrinter", points)

class PointWriterDecorator(PointWriter):
  """A base class for PointWriters which send their output to another PointWriter. This class should be inherited from"""
  def __init__(self, output: PointWriter):
    self.output = output

  def flush(self):
    """Flush any internal state, i.e. make sure that any internally stored or buffered messages are transmitted and then flush the output"""
    self._flush()
    self.output.flush()
  
  def _flush(self):
    pass

class PointToTelemessageWriter(PointWriter):
  """A base class for PointWriters which send their output to a TelemessageWriter. This class should be inherited from"""
  def __init__(self, output: TelemessageWriter):
    self.output = output

  def flush(self):
    """Flush any internal state, i.e. make sure that any internally stored or buffered messages are transmitted and then flush the output"""
    self._flush()
    self.output.flush()
  
  def _flush(self):
    pass