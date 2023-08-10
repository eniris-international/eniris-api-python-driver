from eniris.telemessage import Telemessage


class TelemessageWriter:
    """TelemessageWriters have a method writeTelemessage which receives a Telemessage.
    Typically, they will perform some operations on the message, and the write it to
    an endpoint.
    """

    def writeTelemessage(self, message: Telemessage):
        raise NotImplementedError("This method should be overridden in child classes")

    def flush(self):
        """Flush any internal state, i.e. make sure that any internally stored or
        buffered messages are transmitted"""


class TelemessagePrinter(TelemessageWriter):
    """A TelemessageWriter which prints any received Telemessages. Helpful for
    development and demonstration purposes"""

    def writeTelemessage(self, message: Telemessage):
        print(f"TelemessagePrinter {message}")
