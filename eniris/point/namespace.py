#!/usr/bin/python
from dataclasses import dataclass, asdict


@dataclass
class Namespace:
    """A namespace in which measurements get stored. A namespace determines who can
    access measurements and the retention of the data stored in the namespace.

    All Influx versions have incorporated this concept in one way or another:
    - In InfluxDB 1 the namespace is determined by both the database and the retention
      policy of a measurement, see:
      https://docs.influxdata.com/influxdb/v1.8/concepts/glossary/#retention-policy-rp
    - In InfluxDB 2 the namespace is determined by the organization and the bucket of
      a measurement, see:
      https://docs.influxdata.com/influxdb/v2.0/organizations/buckets/
    - In InfluxDB 3 the namespace is specified directly using a single string, see:
      https://github.com/influxdata/influxdb_iox#use-influxdb-20-api-compatibility

    Attaching a namespace abstracts which InfluxDB version is used for storage and thus
    allows to construct points in a consistent way over all versions.
    """

    @staticmethod
    def version() -> str:
        """Return a version string corresponding to the namespace type"""
        raise NotImplementedError(
            "This function must be implemented by all Namespace subclasses"
        )

    @staticmethod
    def create(**kwargs):
        """A method which allows to construct a concrete Namespace objects based on the
        passed keywords. If no Namespace can be constructed, a ValueError or TypeError is
        raised

        Args:
          - keyword arguments 'database' and 'retentionPolicy' for a V1Namespace
          - keyword arguments 'organization' and 'bucket' for a V2Namespace
          - keyword argument 'name' for a V3Namespace

        Returns:
          V1Namespace|V2Namespace|V3Namespace: The namespace corresponding to the
            passed keyword arguments

        Example:
        >>> from eniris.point import Namespace
        >>> Namespace.create(database='myDatabase', retentionPolicy='myRetentionPolicy')
        V1Namespace(database='myDatabase', retentionPolicy='myRetentionPolicy')
        >>> Namespace.create(organization='myOrganization', bucket='myBucket')
        V2Namespace(organization='myOrganization', bucket='myBucket')
        >>> Namespace.create(name='myNamespace')
        V3Namespace(name='myNamespace')
        """
        if "database" in kwargs and "retentionPolicy" in kwargs:
            return V1Namespace(
                database=kwargs["database"], retentionPolicy=kwargs["retentionPolicy"]
            )
        if "organization" in kwargs and "bucket" in kwargs:
            return V2Namespace(
                organization=kwargs["organization"], bucket=kwargs["bucket"]
            )
        if "name" in kwargs:
            return V3Namespace(name=kwargs["name"])
        raise ValueError("Unable to detect the namespace type")

    def toUrlParameters(self) -> "dict[str, str]":
        """A method which returns which url parameters should be attached to a
        POST request which is storing data for a namespace

        Returns:
          dict[str, str]: The url parameters required to store data in
            the specified namespace
        """
        raise NotImplementedError("This method should be overrriden in child classes")

    def toJson(self):
        """A JSON dumpable representation of the Namespace object, which can be
        converted back into the object using the Namespace.fromJson method"""
        return {**asdict(self), "version": self.version()}


@dataclass
class V1Namespace(Namespace):
    """The InfluxDB 1 implementation of the namespace concept,
    i.e. the combination of a 'database' and 'retentionPolicy'"""

    database: str
    retentionPolicy: str

    @staticmethod
    def version() -> str:
        return "1"

    @staticmethod
    def validateDatabase(database: str) -> None:
        """Check wether the argument is a valid database name

        Args:
            value: Anything really

        Returns:
            None: An exception is raised when the argument is not a valid database name
        """
        if not isinstance(database, str):
            raise TypeError("Database must be a string")
        if len(database) == 0:  # Not required by Influx, but required by Eniris
            raise ValueError("Database must have a length of at least one character")

    @property  # type: ignore
    def database(self):
        """Get the database of the namespace

        Returns:
            str
        """
        return self._database

    @database.setter
    def database(self, database: str):
        """Set the database of the namespace

        Args:
            database (str): A valid database name
        """
        V1Namespace.validateDatabase(database)
        self._database = database

    @staticmethod
    def validateRetentionPolicy(retentionPolicy: str) -> None:
        """Check wether the argument is a valid retention policy name

        Args:
            retentionPolicy: Anything really

        Returns:
            None: An exception is raised when the argument is not a valid \
              retention policy name
        """
        if not isinstance(retentionPolicy, str):
            raise TypeError("Retention policy must be a string")
        if len(retentionPolicy) == 0:  # Not required by Influx, but required by Eniris
            raise ValueError(
                "Retention policy must have a length of at least one character"
            )

    @property  # type: ignore
    def retentionPolicy(self):
        """Get the retention policy of the namespace

        Returns:
            str
        """
        return self._retentionPolicy

    @retentionPolicy.setter
    def retentionPolicy(self, retentionPolicy: str):
        """Set the retention policy of the namespace

        Args:
            retentionPolicy (str): A valid retention policy name
        """
        V1Namespace.validateRetentionPolicy(retentionPolicy)
        self._retentionPolicy = retentionPolicy

    def toUrlParameters(self):
        return {"db": self._database, "rp": self._retentionPolicy}


@dataclass
class V2Namespace(Namespace):
    """The InfluxDB 2 implementation of the namespace concept, i.e.
    the combination of an 'organization' and a 'bucket'"""

    organization: str
    bucket: str

    @staticmethod
    def version() -> str:
        return "2"

    @staticmethod
    def validateOrganization(organization: str) -> None:
        """Check wether the argument is a valid organization name

        Args:
            organization: Anything really

        Returns:
            None: An exception is raised when the argument is not a \
              valid organization name
        """
        if not isinstance(organization, str):
            raise TypeError("Organization must be a string")
        # Not required by Influx, but required by Eniris
        if len(organization) == 0:
            raise ValueError(
                "Organization must have a length of at least one character"
            )

    @property  # type: ignore
    def organization(self):
        """Get the organization of the namespace

        Returns:
            str
        """
        return self._organization

    @organization.setter
    def organization(self, organization: str):
        """Set the organization of the namespace

        Args:
            organization (str): A valid organization name
        """
        V2Namespace.validateOrganization(organization)
        self._organization = organization

    @staticmethod
    def validateBucket(bucket: str) -> None:
        """Check wether the argument is a valid bucket name

        Args:
            bucket: Anything really

        Returns:
            None: An exception is raised when the argument is not a valid bucket name
        """
        if not isinstance(bucket, str):
            raise TypeError("Bucket must be a string")
        # Not required by Influx, but required by Eniris
        if len(bucket) == 0:
            raise ValueError("Bucket must have a length of at least one character")

    @property  # type: ignore
    def bucket(self):
        """Get the bucket of the namespace

        Returns:
            str
        """
        return self._bucket

    @bucket.setter
    def bucket(self, bucket: str):
        """Set the bucket of the namespace

        Args:
            bucket (str): A valid bucket name
        """
        V2Namespace.validateBucket(bucket)
        self._bucket = bucket

    def toUrlParameters(self):
        return {"org": self._organization, "bucket": self._bucket}


@dataclass
class V3Namespace(Namespace):
    """The InfluxDB 3 implementation of the namespace concept, i.e. a simple string"""

    name: str

    @staticmethod
    def version() -> str:
        return "IOx"

    @staticmethod
    def validateName(name: str):
        """Check wether the argument is a valid namespace name

        Args:
            name: Anything really

        Returns:
            None: An exception is raised when the argument is not a valid namespace name
        """
        if not isinstance(name, str):
            raise TypeError("Name must be a string")
        # Not required by Influx, but required by Eniris
        if len(name) == 0:
            raise ValueError("Name must have a length of at least one character")

    @property  # type: ignore
    def name(self):
        """Get the name of the namespace

        Returns:
            str
        """
        return self._name

    @name.setter
    def name(self, name: str):
        """Set the name of the namespace

        Args:
            name (str): A valid namespace name
        """
        V3Namespace.validateName(name)
        self._name = name

    def toUrlParameters(self):
        return {"namespace": self._name}
