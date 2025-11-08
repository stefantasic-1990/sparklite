from sparklite.rdd import RDD

class DummyRDD(RDD):
    def __init__(self, id, op="dummy_op", parents=(), num_of_parts=1):
        super().__init__(id=id, op=op, parents=parents, num_of_parts=num_of_parts)

def test_short_id_length_and_format():
    rdd = DummyRDD(id="abcdefg123456789")
    lineage = rdd.get_ascii_lineage()
    print(lineage)

test_short_id_length_and_format()