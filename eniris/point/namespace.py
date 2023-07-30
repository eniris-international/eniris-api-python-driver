#!/usr/bin/python
from dataclasses import dataclass, asdict

@dataclass
class Namespace:
  @staticmethod
  def fromJson(**kwargs):
    if "database" in kwargs and "retentionPolicy" in kwargs:
      return V1Namespace(**kwargs)
    elif "organization" in kwargs and "bucket" in kwargs:
      return V2Namespace(**kwargs)
    elif "name" in kwargs:
      return V3Namespace(**kwargs)
    else:
      raise ValueError("Unable to detect the namespace type")
    

  def toUrlParameters(self) -> 'dict[str, str]':
    raise NotImplementedError("This method should be overrriden in child classes")
  
  def toJson(self):
    return asdict(self)

@dataclass
class V1Namespace(Namespace):
  database: str
  retentionPolicy: str

  def __init__(self, database: str, retentionPolicy: str):
    V1Namespace.validateDatabase(database)
    self._database = database
    V1Namespace.validateRetentionPolicy(retentionPolicy)
    self._retentionPolicy = retentionPolicy

  @staticmethod
  def validateDatabase(s: str):
    if not isinstance(s, str):
      raise ValueError("Database must be a string")
    if len(s) == 0: # Not required by Influx, but required by Eniris
      raise ValueError("Database must have a length of at least one character")
    
  @property
  def database(self):
    return self._database
  
  @database.setter
  def database(self, s: str):
    V1Namespace.validateDatabase(s)
    self._database = s

  @staticmethod
  def validateRetentionPolicy(s: str):
    if not isinstance(s, str):
      raise ValueError("Retention policy must be a string")
    if len(s) == 0: # Not required by Influx, but required by Eniris
      raise ValueError("Retention policy must have a length of at least one character")
    
  @property
  def retentionPolicy(self):
    return self._retentionPolicy
  
  @retentionPolicy.setter
  def retentionPolicy(self, s: str):
    V1Namespace.validateRetentionPolicy(s)
    self._retentionPolicy = s

  def toUrlParameters(self):
    return {"db": self._database, "rp": self._retentionPolicy}
  
@dataclass
class V2Namespace(Namespace):
  organization: str
  bucket: str

  def __init__(self, organization: str, bucket: str):
    V2Namespace.validateOrganization(organization)
    self._organization = organization
    V2Namespace.validateBucket(bucket)
    self._bucket = bucket

  @staticmethod
  def validateOrganization(s: str):
    if not isinstance(s, str):
      raise ValueError("Organization must be a string")
    if len(s) == 0: # Not required by Influx, but required by Eniris
      raise ValueError("Organization must have a length of at least one character")
    
  @property
  def organization(self):
    return self._organization
  
  @organization.setter
  def organization(self, s: str):
    V2Namespace.validateOrganization(s)
    self._organization = s

  @staticmethod
  def validateBucket(s: str):
    if not isinstance(s, str):
      raise ValueError("Bucket must be a string")
    if len(s) == 0: # Not required by Influx, but required by Eniris
      raise ValueError("Bucket must have a length of at least one character")
    
  @property
  def bucket(self):
    return self._bucket
  
  @bucket.setter
  def bucket(self, s: str):
    V2Namespace.validateBucket(s)
    self._bucket = s

  def toUrlParameters(self):
    return {"org": self._organization, "bucket": self._bucket}
  
@dataclass
class V3Namespace(Namespace):
  name: str

  def __init__(self, name: str):
    V3Namespace.validateName(name)
    self._name = name

  @staticmethod
  def validateName(s: str):
    if not isinstance(s, str):
      raise ValueError("Name must be a string")
    if len(s) == 0: # Not required by Influx, but required by Eniris
      raise ValueError("Name must have a length of at least one character")
    
  @property
  def name(self):
    return self._name
  
  @name.setter
  def name(self, s: str):
    V3Namespace.validateName(s)
    self._name = s

  def toUrlParameters(self):
    return {"namespace": self._name}