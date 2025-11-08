from sparklite.rdd import RDD

def test_short_id_length_and_format():
    rdd1 = RDD(
        id="abcdefg123456789",
        op="dummy_op",
        parents=(),
        num_of_parts=3
    )
    rdd1.print_lineage()