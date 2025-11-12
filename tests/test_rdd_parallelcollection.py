from sparklite.rdd import ParallelCollectionRDD

def test_partition_distribution_shape_and_determinism():
    """Round-robin partitioning policy is correct and reproducible."""
    data = list(range(0, 16))
    num_of_partitions = 3
    reconstructed_data = []
    rdd1 = ParallelCollectionRDD(data=list(range(0, 16)), num_of_partitions=3)
    rdd2 = ParallelCollectionRDD(data=list(range(0, 16)), num_of_partitions=3)

    for partition_index in range(num_of_partitions):
        partition = list(rdd1.compute(partition_index))
        reconstructed_data += partition
        assert partition == list(range(partition_index, 16, 3)), "Partitions contain invalid elements."
        assert partition == list(rdd2.compute(partition_index)), "Partitions should be reproducible given identical inputs."
    assert set(reconstructed_data) == set(data), "The union of all partitions should equal the input dataset."

def test_empty_partition_processing():
    """Calling compute on empty partitions produces no errors."""
    data = [1]
    num_of_partitions = 4
    rdd = ParallelCollectionRDD(data=data, num_of_partitions=num_of_partitions)
    partition_lens = list(range(num_of_partitions))

    for partition_index in range(num_of_partitions):
        partition = list(rdd.compute(partition_index))
        partition_lens[partition_index] = len(partition)
    
    assert partition_lens == [1, 0, 0, 0], "Invalid handling of empty partitions."

def test_eager_generator_consumption_and_persistence():
    """RDD immediately consumes and persists data described by a generator."""
    iter_count = [0]

    def gen():
        n = 1
        while n <= 10:
            yield n
            iter_count[0] += 1
            n += 1

    gen = gen()
    rdd1 = ParallelCollectionRDD(data=gen, num_of_partitions=2)

    assert iter_count[0] == 10, "Data generator not consumed in entirety on RDD creation."
    assert list(rdd1.compute(0)) == [1, 3, 5, 7, 9], "Data not correctly persisted."
    assert list(rdd1.compute(1)) == [2, 4, 6, 8, 10], "Data not correctly persisted."

    rdd2 = ParallelCollectionRDD(data=gen, num_of_partitions=2)

    assert list(rdd2.compute(0)) == [], "Consuming the same generator for another RDD should yield no results."