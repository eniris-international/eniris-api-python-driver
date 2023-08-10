from dataclasses import dataclass, asdict


@dataclass
class Telemessage:
    """A message with lines of telemetry which must be transmitted as a single block
    (telemetry messages), including url parameters which must be included in the
    transmission"""

    parameters: "dict[str, str]"
    lines: "list[bytes]"

    def __init__(self, parameters: "dict[str, str]", lines: "list[bytes]"):
        """Constructor

        Args:
          params (dict[str, str]): Query parameters which should be added to the post\
            request in which the telemessage lines are transmitted
          lines (list[bytes]): A list of bytes, where each byte is a lineprotocol\
            encoded point
        """
        self.parameters = parameters
        self.lines = lines

    def nrBytes(self):
        """Get the total number of bytes of the lines in the telemessage

        Returns:
            int: The total number of bytes of the lines in the telemessage
        """
        return sum(len(line) for line in self.lines)

    def toJson(self):
        """Return an (almost) JSON dumpable representation of the telemessage

        Returns:
            A dictionary with a property 'parameters' and a property 'lines'
        """
        return asdict(self)
