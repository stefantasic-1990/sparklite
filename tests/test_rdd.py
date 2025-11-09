import re
from sparklite.rdd import RDD

class DummyRDD(RDD):
    def __init__(self, op="dummy_op", parents=(), num_of_parts=1):
        super().__init__(op=op, parents=parents, num_of_parts=num_of_parts)

def test_short_id_length_and_format():
    """
    First 16 characters of shortened RDD ID appear within lineage output and are lowercase hex.
    """
    rdd = DummyRDD()
    lineage = rdd.get_ascii_lineage()
    short_id = re.findall(r"\[(.*?)\]", lineage)[0]
    assert len(short_id) == 16, "Short RDD ID must be 16 characters."
    assert all(c in "0123456789abcdef" for c in short_id), "Short RDD ID must be lowercase hex."

