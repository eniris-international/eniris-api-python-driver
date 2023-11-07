from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class Telemessage:
    """A message with lines of telemetry which must be transmitted as a single block
    (telemetry messages), including url parameters which must be included in the
    transmission"""

    parameters: "dict[str, str]"
    data: "bytes"
    headers: "dict[str,str]|None"

    def __init__(
        self,
        parameters: "dict[str, str]",
        data: "list[bytes]|bytes",
        headers: "Optional[dict[str, str]]" = None,
    ):
        """Constructor

        Args:
          params (dict[str, str]): Query parameters which should be added to the post\
            request in which the telemessage lines are transmitted
          data (list[bytes]|bytes): A list of bytes or bytes, where each element is a lineprotocol\
            encoded point
        """
        self.parameters = parameters
        if isinstance(data, list):
            self.data = b"\n".join(data)
        else:
            self.data = data
        if headers is None:
            self.headers = {}
        else:
            self.headers = headers

    def nrBytes(self):
        """Get the total number of bytes of the lines in the telemessage

        Returns:
            int: The total number of bytes of the lines in the telemessage
        """
        return len(self.data)

    def toJson(self):
        """Return an (almost) JSON dumpable representation of the telemessage

        Returns:
            A dictionary with a property 'parameters' and a property 'lines'
        """
        return asdict(self)
