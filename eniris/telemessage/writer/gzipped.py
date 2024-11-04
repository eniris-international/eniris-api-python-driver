from eniris.telemessage.writer.writer import TelemessageWriter
from eniris.telemessage import Telemessage

import gzip


class GZipTelemessageWriter:
    """
    TelemessageWriter that compresses Telemessages using the gzip algorithm,
    and then passes them on to another TelemessageWriter. If the compression does
    not have a net positive effect in the amount of data send, then the
    original uncompressed message is sent.
    """

    def __init__(self, output: TelemessageWriter, compresslevel: int = 9):
        """Optional argument is the compression level, in range of 0-9.

        Args:
            output (TelemessageWriter): _description_
        """
        self.output = output
        self.compresslevel = compresslevel

    def writeTelemessage(self, message: Telemessage):
        gzippedData = gzip.compress(message.data, compresslevel=self.compresslevel)
        ammendedHeaders = {**message.headers, "Content-Encoding": "gzip"}
        gzippedMessage = Telemessage(
            message.parameters, gzippedData, headers=ammendedHeaders
        )

        if len(message.data) > len(gzippedData) + 23:
            # If the gzipped message is more efficient, then write this
            # 23 is the amount of bytes used for the Content-Encoding header
            self.output.writeTelemessage(gzippedMessage)
        else:
            # If it is not more efficient, then don't use compression
            self.output.writeTelemessage(message)
        
    def close(self):
        self.output.close()

    def flush(self):
        self.output.flush()
