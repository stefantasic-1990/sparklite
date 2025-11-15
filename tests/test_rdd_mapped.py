import pytest
from sparklite.rdd import MappedRDD, ParallelCollectionRDD

def test_output_partition_correctness():
    """Output partitions produced by 'compute()' preserve element order and membership."""
    rdd1 = ParallelCollectionRDD(data=[1, 2, 3, 4], num_of_partitions=2)
    rdd2 = rdd1.map(function=lambda x: x + 1)
    partition1 = list(rdd2.compute(0))
    partition2 = list(rdd2.compute(1))
    assert partition1 == [2, 4], "Unexpected output partition element order or membership."
    assert partition2 == [3, 5], "Unexpected output partition element order or membership."

def test_lazy_evaluation():
    """Calling 'map()' does not perform processing."""
    rdd1 = ParallelCollectionRDD(data=[1, 2, 3, 4], num_of_partitions=2)
    rdd2 = rdd1.map(function=lambda x: x / 0)