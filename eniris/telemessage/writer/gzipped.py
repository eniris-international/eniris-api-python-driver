from eniris.telemessage.writer.writer import TelemessageWriter
from eniris.telemessage import Telemessage

import gzip


class GZipTelemessageWriter:
    """TelemessageWriter that compresses Telemessages using the gzip algorithm, and then passes them on to another TelemessageWriter"""

    def __init__(self, output: TelemessageWriter, compresslevel: int = 9):
        """Optional argument is the compression level, in range of 0-9.

        Args:
            output (TelemessageWriter): _description_
        """
        self.output = output
        self.compresslevel = compresslevel

    def writeTelemessage(self, message: Telemessage):
        gzipped_data = gzip.compress(message.data, compresslevel=self.compresslevel)
        ammended_headers = {**message.headers, "Content-Encoding": "gzip"}
        gzipped_message = Telemessage(
            message.parameters, gzipped_data, headers=ammended_headers
        )

        self.output.writeTelemessage(gzipped_message)

    def flush(self):
        self.output.flush()
