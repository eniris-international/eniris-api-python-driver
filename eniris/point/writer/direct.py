from eniris.point import Point
from eniris.point.writer import PointToTelemessageWriter
from eniris.telemessage import Telemessage
from eniris.telemessage.writer import TelemessageWriter

class DirectPointToTelemessageWriter(PointToTelemessageWriter):
  def __init__(self, output: TelemessageWriter, maximumBatchSizeBytes:int=1_000_000):
    super().__init__(output)
    self.maximumBatchSizeBytes = maximumBatchSizeBytes
  
  def writePoints(self, points: 'list[Point]'):
    params2data: 'dict[frozenset[tuple[str, str]], list[Point]]' = dict()
    for dp in points:
      dpParams = dp.namespace.toUrlParameters()
      dpParamsKey = frozenset((key, dpParams[key]) for key in dpParams)
      if dpParamsKey not in params2data:
          params2data[dpParamsKey] = list()
      params2data[dpParamsKey].append(dp)

    for params in params2data:
      params_dict = {p[0]: p[1] for p in params}
      params_data = params2data[params]
      cur_bytes: 'list[bytes]' = list()
      cur_bytes_len = 0
      for dp in params_data:
        dp_bytes: bytes = dp.toLineProtocol().encode("utf-8")
        if len(cur_bytes) != 0 and cur_bytes_len + len(dp_bytes) > self.maximumBatchSizeBytes:
          self.output.writeTelemessage(Telemessage(params_dict, cur_bytes))
          cur_bytes = list()
          cur_bytes_len = 0
        cur_bytes.append(dp_bytes)
        cur_bytes_len += len(dp_bytes) + 1
      self.output.writeTelemessage(Telemessage(params_dict, cur_bytes))
  
  def stop(self):
    pass