from dataclasses import dataclass, asdict

@dataclass
class Telemessage(object):
  """A message with lines of telemetry which must be transmitted as a single block (telemetry messages), including url parameters which must be included in the transmission, and the number of failed transmission attempts"""
  parameters: dict[str, str]
  lines: list[bytes]

  def __init__(self, parameters: dict[str, str], lines: list[bytes]):
    self.parameters = parameters
    self.lines = lines

  def nrBytes(self):
    return sum(len(line) for line in self.lines)

  def toJson(self):
    return asdict(self)