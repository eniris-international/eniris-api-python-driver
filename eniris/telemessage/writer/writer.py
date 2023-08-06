from eniris.telemessage import Telemessage
  
class TelemessageWriter:
  """TelemessageWriters have a method writeTelemessage which receives a Telemessage.
  Typically, they will perform some operations on the message, and the write it to an endpoint.
  """

  def writeTelemessage(self, tm: Telemessage):
    raise NotImplementedError("This method should be overridden in child classes")
  
  def flush(self):
    """Flush any internal state, i.e. make sure that any internally stored or buffered messages are transmitted"""
    pass
  
class TelemessagePrinter(TelemessageWriter):
  """A TelemessageWriter which prints any received Telemessages. Helpful for development and demonstration purposes"""
  
  def writeTelemessage(self, tm: Telemessage):
    print("TelemessagePrinter", tm)