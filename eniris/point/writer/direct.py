from eniris.point import Point
from eniris.point.writer import PointToTelemessageWriter
from eniris.telemessage import Telemessage
from eniris.telemessage.writer import TelemessageWriter

class DirectPointToTelemessageWriter(PointToTelemessageWriter):
  """The DirectPointToTelemessageWriter class is a PointWriter which takes in Point objects, groups them based on their namespaces
  and then writes Telemessage objects (batched by byte size) to a provided TelemessageWriter.
  It makes sure that length of the telemessages does not exceed a configurable maximum number of bytes.

  Args:
      maximumBatchSizeBytes (int, optional): The maximum size of the combined lines in a single outputted TeleMessage in bytes. Defaults to 10_000_000
  
  Example:
    >>> from eniris.point import Point
    >>> from eniris.point.writer import DirectPointToTelemessageWriter
    >>> from eniris.telemessage.writer import TelemessagePrinter
    >>> from datetime import datetime
    >>>
    >>> ns = {'database': 'myDatabase', 'retentionPolicy': 'myRetentionPolicy'}
    >>> pLiving0 = Point(ns, 'homeSensors', datetime(2023, 1, 1), {'id': 'livingroomSensor'}, {'temp_C': 18., 'humidity_perc': 20.})
    >>> pSauna0 = Point(ns, 'homeSensors', datetime(2023, 1, 1), {'id': 'saunaSensor'}, {'temp_C': 40., 'humidity_perc': 90.})
    >>>
    >>> writer = DirectPointToTelemessageWriter(TelemessagePrinter(), maximumBatchSizeBytes=50)
    >>> writer.writePoints([pLiving0, pSauna0])
    TelemessagePrinter Telemessage(parameters={'db': 'myDatabase', 'rp': 'myRetentionPolicy'}, lines=[b'homeSensors,id=livingroomSensor temp_C=18.0,humidity_perc=20.0 1672527600000000000'])
    TelemessagePrinter Telemessage(parameters={'db': 'myDatabase', 'rp': 'myRetentionPolicy'}, lines=[b'homeSensors,id=saunaSensor temp_C=40.0,humidity_perc=90.0 1672527600000000000'])
  """
  def __init__(self, output: TelemessageWriter, maximumBatchSizeBytes:int=10_000_000):
    super().__init__(output)
    self.maximumBatchSizeBytes = maximumBatchSizeBytes
  
  def writePoints(self, points: 'list[Point]'):
    """Convert points to Telemessage's and writes them to the output, ensuring each telemessage contains points with the same namespace.

    Args:
        points (list[eniris.point.Point]): List of Point's
    """
    params2data: 'dict[frozenset[tuple[str, str]], list[Point]]' = dict()
    for p in points:
      namespaceParams = p.namespace.toUrlParameters()
      namespaceParamsKey = frozenset((key, namespaceParams[key]) for key in namespaceParams)
      params2data.setdefault(namespaceParamsKey, []).append(p)

    for params in params2data:
      params_dict = {p[0]: p[1] for p in params}
      params_data = params2data[params]
      cur_bytes: 'list[bytes]' = list()
      cur_bytes_len = 0
      for p in params_data:
        p_bytes: bytes = p.toLineProtocol().encode("utf-8")
        if len(cur_bytes) != 0 and cur_bytes_len + len(p_bytes) + 1 > self.maximumBatchSizeBytes: # + 1 to take into account the newline character when the lines in the telemessage are joined
          self.output.writeTelemessage(Telemessage(params_dict, cur_bytes))
          cur_bytes = list()
          cur_bytes_len = 0
        cur_bytes.append(p_bytes)
        cur_bytes_len += len(p_bytes) + 1 # Again, we do take the newline character into account
      self.output.writeTelemessage(Telemessage(params_dict, cur_bytes))