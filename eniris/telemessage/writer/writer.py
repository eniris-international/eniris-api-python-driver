from eniris.telemessage import Telemessage
  
class TelemessageWriter:
  """A base class for writing telemessages (telemetry messages). This class should be inherited from"""
  def writeTelemessage(self, tm: Telemessage):
    raise NotImplementedError("This method should be overrriden in child classes")
  
  def flush(self):
    """Flush any internal state, i.e. make sure that any internally stored or buffered messages are transmitted"""
    pass
  
class TelemessagePrinter(TelemessageWriter):
  """A class for debugging which will print any telemessages (telemetry messages) which are passed to it"""
  def writeTelemessage(self, tm: Telemessage):
    pass#print("TelemessagePrinter", tm)

  def __del__(self):
    self.flush()