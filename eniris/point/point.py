#!/usr/bin/python
from dataclasses import dataclass
from collections import UserDict
from collections.abc import Mapping
from datetime import datetime
import math

from eniris.point.namespace import Namespace

class TagSet(UserDict):
  """A set of measured values.
  Since a TagSet is created automatically when passing a dictionary as the 'tags' argument of the Point constructor, one usually does not have to instantiate this class directly.
  
  This corresponds to an InfluxDB tag set, see also: https://docs.influxdata.com/influxdb/v2.6/reference/key-concepts/data-elements/"""

  def __setitem__(self, key: str, value: str):
    """Set a tag set key-value pair

    Args:
        key (str): A valid tag key
        value (str): A valid tag value
    """
    TagSet.validateKey(key)
    TagSet.validateValue(value)
    super().__setitem__(key, value)

  @staticmethod
  def validateKey(key: str):
    """Check wether the argument is a valid tag key

    Args:
        key: Anything really

    Returns:
        None: An exception is raised when the argument is not a valid tag key
    """
    if not isinstance(key, str):
      raise TypeError("Tag key must be a string")
    if len(key) == 0: # Not required by Influx, but required by Eniris
      raise ValueError("Tag key must have a length of at least one character")
    if '\n' in key: # The docs state: 'Lines separated by the newline character \n represent a single point in InfluxDB. Line protocol is whitespace sensitive.' See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/
      raise ValueError("Newline characters are not allowed in tag keys")
    if key[0] == "_": # See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#naming-restrictions
      raise ValueError("Tag key cannot start with an underscore character")

  @staticmethod
  def validateValue(value: str):
    """Check wether the argument is a valid tag value

    Args:
        value: Anything really

    Returns:
        None: An exception is raised when the argument is not a valid tag value
    """
    if not isinstance(value, str):
      raise TypeError("Tag value must be a string")
    if len(value) == 0: # Not required by Influx, but required by Eniris
      raise ValueError("Tag values must have a length of at least one character")
    if '\n' in value: # The docs state: 'Lines separated by the newline character \n represent a single point in InfluxDB. Line protocol is whitespace sensitive.' See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/
      raise ValueError("Newline characters are not allowed in tag values")

  @staticmethod
  def escapeKey(s: str):
    """Convert a tag key into its line-protocol representation, escaping any problematic characters
    See also: https://docs.influxdata.com/influxdb/v2.6/reference/syntax/line-protocol

    Args:
        key (str): A valid tag key

    Returns:
        str: The line-protocol representation of the tag key
    """
    s = s.replace("\\", "\\\\") # Not strictly required, but best to do to avoid nonsense when a tag key ends with a backslash. https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#escaping-backslashes
    return s.replace(",", "\,").replace("=", "\=").replace(" ", "\ ") # See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#special-characters

  @staticmethod
  def escapeValue(s: str):
    """Convert a tag value into its line-protocol representation
    See also: https://docs.influxdata.com/influxdb/v2.6/reference/syntax/line-protocol

    Args:
        value (str): A valid tag value

    Returns:
        str: The line-protocol representation of the tag value
    """
    s = s.replace("\\", "\\\\") # Not strictly required, but best to do to avoid nonsense when a tag value ends with a backslash. https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#escaping-backslashes
    return s.replace(",", "\,").replace("=", "\=").replace(" ", "\ ") # See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#special-characters   

  def toLineProtocol(self):
    """Convert a tag set into its line-protocol representation
    See also: https://docs.influxdata.com/influxdb/v2.6/reference/syntax/line-protocol

    Returns:
        str: The line-protocol representation of the tag set
    """
    lst = [f"{TagSet.escapeKey(k)}={TagSet.escapeValue(self[k])}" for k in self]
    lst.sort()
    return ','.join(lst)
  
class FieldSet(UserDict[str, 'bool|int|float|str']):
  """A set of measured values.
  Since a FieldSet is created automatically when passing a dictionary as the 'fields' argument of the Point constructor, one usually does not have to instantiate this class directly.
  
  This corresponds to an InfluxDB field set, see also: https://docs.influxdata.com/influxdb/v2.6/reference/key-concepts/data-elements/"""

  def __setitem__(self, key: str, value: 'bool|int|float|str'):
    """Set a field set key-value pair

    Args:
        key (str): A valid tag key
        value (str): A valid tag value
    """
    FieldSet.validateKey(key)
    FieldSet.validateValue(value)
    super().__setitem__(key, value)

  @staticmethod
  def validateKey(key: str):
    """Check wether the argument is a valid field key

    Args:
        key: Anything really

    Returns:
        None: An exception is raised when the argument is not a valid field key
    """
    if not isinstance(key, str):
      raise TypeError("Field key must be a string")
    if len(key) == 0: # Not required by Influx, but required by Eniris
      raise ValueError("Field key must have a length of at least one character")
    if '\n' in key: # The docs state: 'Lines separated by the newline character \n represent a single point in InfluxDB. Line protocol is whitespace sensitive.' See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/
      raise ValueError("Newline characters are not allowed in field keys")
    if key[0] == "_": # See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#naming-restrictions
      raise ValueError("Field key cannot start with an underscore character")
    
  @staticmethod
  def validateValue(value: 'bool|int|float|str'):
    """Check wether the argument is a valid field value

    Args:
        value: Anything really

    Returns:
        None: An exception is raised when the argument is not a valid field value
    """
    if isinstance(value, bool):
      pass
    elif isinstance(value, int):
      pass
    elif isinstance(value, float):
      return math.isfinite(value)
    elif isinstance(value, str):
      if '\n' in value: # The docs state: 'Lines separated by the newline character \n represent a single point in InfluxDB. Line protocol is whitespace sensitive.' See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/
        raise ValueError("Newline characters are not allowed in field values")
    else:
      raise TypeError(f"Field value {str(value)} is of the type {str(type(value))}")
    
  @staticmethod
  def escapeKey(key: str):
    """Convert a field key into its line-protocol representation, escaping any problematic characters
    See also: https://docs.influxdata.com/influxdb/v2.6/reference/syntax/line-protocol

    Args:
        key (str): A valid field key

    Returns:
        str: The line-protocol representation of the field key
    """
    key = key.replace("\\", "\\\\") # Not strictly required, but best to do to avoid nonsense when a field key ends with a backslash. https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#escaping-backslashes
    return key.replace(",", "\,").replace("=", "\=").replace(" ", "\ ") # See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#special-characters

  @staticmethod
  def escapeValue(value: 'bool|int|float|str'):
    """Convert a field value into its line-protocol representation
    See also: https://docs.influxdata.com/influxdb/v2.6/reference/syntax/line-protocol

    Args:
        value (str): A valid field value

    Returns:
        str: The line-protocol representation of the field value
    """
    if isinstance(value, bool):
      if value:
        return"T"
      else:
        return "F"
    elif isinstance(value, int):
      return f"{value}i"
    elif isinstance(value, float):
      return str(value)
    elif isinstance(value, str):
      return '"' + str(value).replace('"', '\\"').replace('\\', '\\\\') + '"' # See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#special-characters

  def toLineProtocol(self):
    """Convert a field set into its line-protocol representation
    See also: https://docs.influxdata.com/influxdb/v2.6/reference/syntax/line-protocol

    Returns:
        str: The line-protocol representation of the field set
    """
    lst = [f"{FieldSet.escapeKey(k)}={FieldSet.escapeValue(self[k])}" for k in self]
    return ','.join(lst)

@dataclass
class Point:
  """One or more measured values sharing a single entity and timestamp.

  A point is stored in a 'measurement' (similar to a table in Excel) which is stored in a namespace.
  Each point has a single timestamp and some tags. These tags identify its source as well as other categorical properties which might be useful when querying the data.
  A point must also specify at least one field, containing the actual measured data.
  
  This corresponds to an InfluxDB point, see also: https://docs.influxdata.com/influxdb/v2.6/reference/key-concepts/data-elements/
  
  Example:
    >>> from eniris.point import point
    >>> 
    >>> from datetime import datetime
    >>>
    >>> ns = {'database': 'myDatabase', 'retentionPolicy': 'myRetentionPolicy'}
    >>> dt = datetime(2023, 1, 1)
    >>> p0 = Point(ns, 'homeSensors', dt, {'id': 'livingroomSensor'}, {'temp_C': 18., 'humidity_perc': 20.})
    >>> p1 = Point(ns, 'homeSensors', dt, {'id': 'saunaSensor'}, {'temp_C': 40., 'humidity_perc': 90.})
    >>> print(p0)
    Point(namespace=V1Namespace(database='myDatabase', retentionPolicy='myRetentionPolicy'), measurement='homeSensors', time=datetime.datetime(2023, 1, 1, 0, 0), tags={'id': 'livingroomSensor'}, fields={'temp_C': 18.0, 'humidity_perc': 20.0})
  """
  namespace: Namespace
  measurement: str
  time: datetime
  tags: TagSet
  fields: FieldSet

  @staticmethod
  def validateNamespace(namespace: Namespace):
    """Check wether the argument is a valid namespace object

    Args:
        namespace: Anything really

    Returns:
        None: An exception is raised when the argument is not a valid namespace object
    """
    if not isinstance(namespace, Namespace):
      raise ValueError("Namespace must be a Namespace object")
    
  @property
  def namespace(self) -> Namespace:
    """Get the namespace of the point

    Returns:
        eniris.point.Namespace
    """
    return self._namespace
  
  @namespace.setter
  def namespace(self, namespace: Namespace|dict):
    """Set the namespace of the point

    Args:
        namespace (eniris.point.Namespace|dict): A namespace object, or a JSON representation of a namespace object
    """
    if isinstance(namespace, dict):
      namespace = Namespace.create(**namespace)
    Point.validateNamespace(namespace)
    self._namespace = namespace

  @staticmethod
  def validateMeasurement(measurement: str):
    """Check wether the argument is a valid measurement name

    Args:
        measurement: Anything really

    Returns:
        None: An exception is raised when the argument is not a valid measurement name
    """
    if not isinstance(measurement, str):
      raise ValueError("Measurement name must be a string")
    if len(measurement) == 0: # Not required by Influx, but required by Eniris
      raise ValueError("Measurement name must have a length of at least one character")
    if '\n' in measurement: # The docs state: 'Lines separated by the newline character \n represent a single point in InfluxDB. Line protocol is whitespace sensitive.' See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/
      raise ValueError("Newline characters are not allowed in measurement name")
    if measurement[0] == "_": # See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#naming-restrictions
      raise ValueError("Measurement name cannot start with an underscore character")

  @staticmethod
  def escapeMeasurement(measurement: str):
    """Convert a measurement name into its line-protocol representation, escaping any problematic characters
    See also: https://docs.influxdata.com/influxdb/v2.6/reference/syntax/line-protocol

    Args:
        measurement (str): A valid measurement name

    Returns:
        str: The line-protocol representation of the measurement name
    """
    measurement = measurement.replace("\\", "\\\\") # Not strictly required, but best to do to avoid nonsense when the measurement name ends with a backslash. https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#escaping-backslashes
    return measurement.replace(",", "\,").replace(" ", "\ ") # See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#special-characters
    
  @property
  def measurement(self):
    """Get the measurement of the point

    Returns:
        str
    """
    return self._measurement
  
  @measurement.setter
  def measurement(self, measurement: str):
    """Set the measurement of the point

    Args:
        measurement (str): A string
    """
    Point.validateMeasurement(measurement)
    self._measurement = measurement

  @staticmethod
  def validateTime(time: 'datetime|None'):
    """Check wether the argument is a valid datetime object

    Args:
        time: Anything really

    Returns:
        None: An exception is raised when the argument is not a valid datetime object
    """
    if time is not None and not isinstance(time, datetime):
      raise ValueError("Time must be either None or a datetime object")
    
  @property
  def time(self):
    """Get the time of the point

    Returns:
        datetime
    """
    return self._time
  
  @time.setter
  def time(self, time: 'str|datetime|None'):
    """Set the time of the point

    Args:
        time (str|datetime|None): A string in the "%Y-%m-%dT%H:%M:%S.%f%z" format, a datetime object or None it the timestamp should be the moment when the data is consumed by the receiving system
    """
    if isinstance(time, str):
      time = datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%f%z")
    Point.validateTime(time)
    self._time = time

  @property
  def tags(self):
    """Get the tag set of the point

    Returns:
        eniris.point.TagSet
    """
    return self._tags
  
  @tags.setter
  def tags(self, tags: 'TagSet|Mapping[str, str]'):
    """Set the tag set of the point

    Args:
        tags (eniris.point.TagSet|Mapping[str, str]): A TagSet object or a Mapping where both the keys and values are strings
    """
    self._tags = tags if isinstance(tags, TagSet) else TagSet(tags)

  @property
  def fields(self):
    """Get the field set of the point

    Returns:
        eniris.point.FieldSet
    """
    return self._fields
  
  @fields.setter
  def fields(self, fields: 'FieldSet|Mapping[str, bool|int|float|str]'):
    """Set the field set of the point

    Args:
        fields (eniris.point.FieldSet|Mapping[str, bool|int|float|str]): A TagSet object or a Mapping where the keys are strings and the values are booleans, integers, floats or strings
    """
    self._fields = fields if isinstance(fields, FieldSet) else FieldSet(fields)

  def toLineProtocol(self):
    """Convert a point into its line-protocol representation
    See also: https://docs.influxdata.com/influxdb/v2.6/reference/syntax/line-protocol

    Returns:
        str: The line-protocol representation of the measurement name
    """
    return Point.escapeMeasurement(self._measurement) \
      + ("," + self._tags.toLineProtocol() if len(self._tags) > 0 else "") \
      + " " + self._fields.toLineProtocol() \
      + (" " + str(int(self._time.timestamp()*1_000_000_000)) if self._time is not None else "")

  def toJson(self):
    """Return a JSON dumpable representation of the telemessage

    Returns:
        A dictionary with the properties 'namespace', 'measurement', 'time', 'tags' and 'fields'.
        The time will be represented as a string in the "%Y-%m-%dT%H:%M:%S.%f%z" format 
    """
    return {
      "namespace": self._namespace.toJson(),
      "measurement": self._measurement,
      "time": self._time.isoformat() if self._time is not None else None,
      "tags": dict(self._tags),
      "fields": dict(self._fields)
    }