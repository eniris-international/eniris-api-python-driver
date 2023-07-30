from eniris.point import Point

class PointWriter:
  def writePoints(self, points: 'list[Point]'):
    raise NotImplementedError("This method should be overrriden in child classes")
  
  def flush(self):
    pass
  
class PointPrinter(PointWriter):
  def writePoints(self, points: 'list[Point]'):
    print("PointPrinter", points)

class PointWriterDecorator(PointWriter):
  def __init__(self, output: PointWriter):
    self.output = output

  def flush(self):
    self._flush()
    self.output.flush()
  
  def _flush(self):
    pass