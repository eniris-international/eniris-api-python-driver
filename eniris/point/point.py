#!/usr/bin/python
from dataclasses import dataclass
from collections import UserDict
from collections.abc import Mapping
from datetime import datetime
import math

from eniris.point.namespace import Namespace

class TagSet(UserDict):
  def __setitem__(self, key: str, value: str):
    TagSet.validateKey(key)
    TagSet.validateValue(value)
    super().__setitem__(key, value)

  @staticmethod
  def validateKey(s: str):
    if not isinstance(s, str):
      raise TypeError("Tag key must be a string")
    if len(s) == 0: # Not required by Influx, but required by Eniris
      raise ValueError("Tag key must have a length of at least one character")
    if '\n' in s: # The docs state: 'Lines separated by the newline character \n represent a single point in InfluxDB. Line protocol is whitespace sensitive.' See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/
      raise ValueError("Newline characters are not allowed in tag keys")
    if s[0] == "_": # See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#naming-restrictions
      raise ValueError("Tag key cannot start with an underscore character")

  @staticmethod
  def validateValue(s: str):
    if not isinstance(s, str):
      raise TypeError("Tag value must be a string")
    if len(s) == 0: # Not required by Influx, but required by Eniris
      raise ValueError("Tag values must have a length of at least one character")
    if '\n' in s: # The docs state: 'Lines separated by the newline character \n represent a single point in InfluxDB. Line protocol is whitespace sensitive.' See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/
      raise ValueError("Newline characters are not allowed in tag values")

  @staticmethod
  def escapeKey(s: str):
    s = s.replace("\\", "\\\\") # Not strictly required, but best to do to avoid nonsense when a tag or field key ends with a backslash. https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#escaping-backslashes
    return s.replace(",", "\,").replace("=", "\=").replace(" ", "\ ") # See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#special-characters

  @staticmethod
  def escapeValue(s: str):
    s = s.replace("\\", "\\\\") # Not strictly required, but best to do to avoid nonsense when a tag value ends with a backslash. https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#escaping-backslashes
    return s.replace(",", "\,").replace("=", "\=").replace(" ", "\ ") # See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#special-characters   

  def toLineProtocol(self):
    lst = [f"{TagSet.escapeKey(k)}={TagSet.escapeValue(self[k])}" for k in self]
    lst.sort()
    return ','.join(lst)
    
class FieldSet(UserDict[str, str]):
  def __setitem__(self, key: str, value: 'bool|int|float|str'):
    FieldSet.validateKey(key)
    FieldSet.validateValue(value)
    super().__setitem__(key, value)

  @staticmethod
  def validateKey(s: str):
    if not isinstance(s, str):
      raise TypeError("Field key must be a string")
    if len(s) == 0: # Not required by Influx, but required by Eniris
      raise ValueError("Field key must have a length of at least one character")
    if '\n' in s: # The docs state: 'Lines separated by the newline character \n represent a single point in InfluxDB. Line protocol is whitespace sensitive.' See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/
      raise ValueError("Newline characters are not allowed in field keys")
    if s[0] == "_": # See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#naming-restrictions
      raise ValueError("Field key cannot start with an underscore character")
    
  @staticmethod
  def validateValue(v: 'bool|int|float|str'):
    if isinstance(v, bool):
      pass
    elif isinstance(v, int):
      pass
    elif isinstance(v, float):
      return math.isfinite(v)
    elif isinstance(v, str):
      if '\n' in v: # The docs state: 'Lines separated by the newline character \n represent a single point in InfluxDB. Line protocol is whitespace sensitive.' See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/
        raise ValueError("Newline characters are not allowed in field values")
    else:
      raise TypeError(f"Field value {str(v)} is of the type {str(type(v))}")
    
  @staticmethod
  def escapeKey(s: str):
    s = s.replace("\\", "\\\\") # Not strictly required, but best to do to avoid nonsense when a tag or field key ends with a backslash. https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#escaping-backslashes
    return s.replace(",", "\,").replace("=", "\=").replace(" ", "\ ") # See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#special-characters

  @staticmethod
  def escapeValue(v: 'bool|int|float|str'):
    if isinstance(v, bool):
      if v:
        return"T"
      else:
        return "F"
    elif isinstance(v, int):
      return f"{v}i"
    elif isinstance(v, float):
      return str(v)
    elif isinstance(v, str):
      return '"' + str(v).replace('"', '\\"').replace('\\', '\\\\') + '"' # See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#special-characters

  def toLineProtocol(self):
    lst = [f"{FieldSet.escapeKey(k)}={FieldSet.escapeValue(self[k])}" for k in self]
    return ','.join(lst)
  
class FieldSet(UserDict[str, 'bool|int|float|str']):
  def __setitem__(self, key: str, value: 'bool|int|float|str'):
    FieldSet.validateKey(key)
    FieldSet.validateValue(value)
    super().__setitem__(key, value)

  @staticmethod
  def validateKey(s: str):
    if not isinstance(s, str):
      raise TypeError("Field key must be a string")
    if len(s) == 0: # Not required by Influx, but required by Eniris
      raise ValueError("Field key must have a length of at least one character")
    if '\n' in s: # The docs state: 'Lines separated by the newline character \n represent a single point in InfluxDB. Line protocol is whitespace sensitive.' See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/
      raise ValueError("Newline characters are not allowed in field keys")
    if s[0] == "_": # See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#naming-restrictions
      raise ValueError("Field key cannot start with an underscore character")
    
  @staticmethod
  def validateValue(v: 'bool|int|float|str'):
    if isinstance(v, bool):
      pass
    elif isinstance(v, int):
      pass
    elif isinstance(v, float):
      return math.isfinite(v)
    elif isinstance(v, str):
      if '\n' in v: # The docs state: 'Lines separated by the newline character \n represent a single point in InfluxDB. Line protocol is whitespace sensitive.' See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/
        raise ValueError("Newline characters are not allowed in field values")
    else:
      raise TypeError(f"Field value {str(v)} is of the type {str(type(v))}")
    
  @staticmethod
  def escapeKey(s: str):
    s = s.replace("\\", "\\\\") # Not strictly required, but best to do to avoid nonsense when a tag or field key ends with a backslash. https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#escaping-backslashes
    return s.replace(",", "\,").replace("=", "\=").replace(" ", "\ ") # See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#special-characters

  @staticmethod
  def escapeValue(v: 'bool|int|float|str'):
    if isinstance(v, bool):
      if v:
        return"T"
      else:
        return "F"
    elif isinstance(v, int):
      return f"{v}i"
    elif isinstance(v, float):
      return str(v)
    elif isinstance(v, str):
      return '"' + str(v).replace('"', '\\"').replace('\\', '\\\\') + '"' # See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#special-characters

  def toLineProtocol(self):
    lst = [f"{FieldSet.escapeKey(k)}={FieldSet.escapeValue(self[k])}" for k in self]
    return ','.join(lst)

@dataclass
class Point:
  namespace: Namespace
  measurement: str
  time: datetime
  tags: TagSet
  fields: FieldSet

  @staticmethod
  def validateNamespace(ns: Namespace):
    if not isinstance(ns, Namespace):
      raise ValueError("Namespace must be a Namespace object")
    
  @property
  def namespace(self):
    return self._namespace
  
  @namespace.setter
  def namespace(self, ns: Namespace):
    if isinstance(ns, dict):
      ns = Namespace.fromJson(**ns)
    Point.validateNamespace(ns)
    self._namespace = ns

  @staticmethod
  def validateMeasurement(s: str):
    if not isinstance(s, str):
      raise ValueError("Measurement name must be a string")
    if len(s) == 0: # Not required by Influx, but required by Eniris
      raise ValueError("Measurement name must have a length of at least one character")
    if '\n' in s: # The docs state: 'Lines separated by the newline character \n represent a single point in InfluxDB. Line protocol is whitespace sensitive.' See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/
      raise ValueError("Newline characters are not allowed in measurement name")
    if s[0] == "_": # See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#naming-restrictions
      raise ValueError("Measurement name cannot start with an underscore character")

  @staticmethod
  def escapeMeasurement(s: str):
    s = s.replace("\\", "\\\\") # Not strictly required, but best to do to avoid nonsense when the measurement name ends with a backslash. https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#escaping-backslashes
    return s.replace(",", "\,").replace(" ", "\ ") # See: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#special-characters
    
  @property
  def measurement(self):
    return self._measurement
  
  @measurement.setter
  def measurement(self, s: str):
    Point.validateMeasurement(s)
    self._measurement = s

  @staticmethod
  def validateTime(dt: 'datetime|None'):
    if dt is not None and not isinstance(dt, datetime):
      raise ValueError("Time must be either None or a datetime object")
    
  @property
  def time(self):
    return self._time
  
  @time.setter
  def time(self, time: 'str|datetime|None'):
    if isinstance(time, str):
      time = datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%f%z")
    Point.validateTime(time)
    self._time = time

  @property
  def tags(self):
    return self._tags
  
  @tags.setter
  def tags(self, tags: 'TagSet|Mapping[str, str]'):
    self._tags = tags if isinstance(tags, TagSet) else TagSet(tags)

  @property
  def fields(self):
    return self._fields
  
  @fields.setter
  def fields(self, fields: 'FieldSet|Mapping[str, bool|int|float|str]'):
    self._fields = fields if isinstance(fields, FieldSet) else FieldSet(fields)

  def toLineProtocol(self):
    return Point.escapeMeasurement(self._measurement) \
      + ("," + self._tags.toLineProtocol() if len(self._tags) > 0 else "") \
      + " " + self._fields.toLineProtocol() \
      + (" " + str(int(self._time.timestamp()*1_000_000_000)) if self._time is not None else "")
  
  def toJson(self):
    return {
      "namespace": self._namespace.toJson(),
      "measurement": self._measurement,
      "time": self._time.isoformat(),
      "tags": dict(self._tags),
      "fields": dict(self._fields)
    }