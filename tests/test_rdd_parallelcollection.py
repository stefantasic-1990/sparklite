from sparklite.rdd import ParallelCollectionRDD

def test_partition_distribution_shape():
    """Round-robin partitioning policy is correct and reproducible."""
    data = list(range(0, 16))
    num_of_partitions = 3
    reconstructed_data = []
    rdd = ParallelCollectionRDD(data=list(range(0, 16)), num_of_partitions=3)

    for partition_index in range(num_of_partitions):
        partition = list(rdd.compute(partition_index))
        reconstructed_data += partition
        assert partition == list(range(partition_index, 16, 3)), "Partitions contain invalid elements."
    assert set(reconstructed_data) == set(data)

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