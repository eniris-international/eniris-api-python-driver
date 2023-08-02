from threading import Lock

from eniris.point import Point
from eniris.point.writer import PointWriterDecorator, PointWriter

class PointDuplicateFilter(PointWriterDecorator):
  def __init__(self, output: PointWriter):
    super().__init__(output)
    self.memory: dict[tuple[frozenset[tuple[str, str]], str, frozenset[tuple[str, str]]], dict[tuple[int, str], bool|int|float|str]] = dict() # {(frozen_namespace, measurement, frozen_tagset) => (time, fieldKey) => fieldValue}
    self.lock = Lock()

  def writePoints(self, points: 'list[Point]'):
    """Filter that filters out measurements that have been pushed before by looking if new measurements are identical to last pushed measurements.

    Args:
        measurements (list): List of measurements

    Returns:
        list: Remaining measurements after filtering
    """
    if len(points) == 0:
      return
    out: 'list[Point]' = []
    with self.lock:
      # Calculate the new memory entries
      new_memory_entries: 'dict[tuple[frozenset[tuple[str, str]], str, frozenset[tuple[str, str]]], dict[tuple[int, str], bool|int|float|str]]' = dict()
      for dp in points:
        namespace_parameters = dp.namespace.toUrlParameters() 
        frozen_namespace = frozenset((key, namespace_parameters[key]) for key in namespace_parameters)
        frozen_tags = frozenset((key, dp.tags[key]) for key in dp.tags)
        key = (frozen_namespace, dp.measurement, frozen_tags)
        if key not in new_memory_entries:
          new_memory_entries[key] = dict()
        new_memory_row_key = new_memory_entries[key]
        for field_name in dp.fields:
          field_value = dp.fields[field_name]
          field_key = (int(dp.time.timestamp()*10**9), field_name)
          new_memory_row_key[field_key] = field_value
      # Create a list of measurements with updates
      for dp in points:
        namespace_parameters = dp.namespace.toUrlParameters() 
        frozen_namespace = frozenset((key, namespace_parameters[key]) for key in namespace_parameters)
        frozen_tags = frozenset((key, dp.tags[key]) for key in dp.tags)
        key = (frozen_namespace, dp.measurement, frozen_tags)
        selected_fields = dict()
        if key not in self.memory:
          self.memory[key] = dict()
        old_memory_row_key = self.memory[key]
        new_memory_row_key = new_memory_entries[key]
        for field_name in dp.fields:
          field_value = dp.fields[field_name]
          field_key = (int(dp.time.timestamp()*10**9), field_name)
          if new_memory_row_key[field_key] == field_value and (field_key not in old_memory_row_key or old_memory_row_key[field_key] != field_value):
            old_memory_row_key[field_key] = field_value
            selected_fields[field_name] = field_value
        if len(selected_fields) > 0:
          out.append(Point(dp.namespace, dp.measurement, dp.time, dp.tags, selected_fields))
      # Update the memory
      self.memory.update(new_memory_entries)
    if len(out) > 0:
      self.output.writePoints(out)