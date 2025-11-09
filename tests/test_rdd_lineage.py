import re
from sparklite.rdd import RDD

class DummyRDD(RDD):
    def __init__(self, op="dummy_op", parents=(), num_of_parts=1):
        super().__init__(op=op, parents=parents, num_of_parts=num_of_parts)

def test_short_id_length_and_format():
    """First 16 characters of shortened RDD ID appear within lineage output and are lowercase hex."""
    rdd = DummyRDD()
    lineage = rdd.get_ascii_lineage()
    short_id = re.findall(r"\[(.*?)\]", lineage)[0]
    assert len(short_id) == 16, "Short RDD ID must be 16 characters."
    assert all(c in "0123456789abcdef" for c in short_id), "Short RDD ID must be lowercase hex."

def test_indentation_and_parts_tag_consistency():
    """Indentation grows by 2 spaces per depth, every line shows (parts=n)."""
    rdd = DummyRDD(num_of_parts=1)
    rdd = DummyRDD(parents=(rdd,), num_of_parts=1)
    rdd = DummyRDD(parents=(rdd,), num_of_parts=2)
    lineage_lines = rdd.get_ascii_lineage().splitlines()

    assert lineage_lines[0].startswith(""), "Depth 0 should start with 0 spaces."
    assert lineage_lines[1].startswith("  "), "Depth 1 should start with 2 spaces."
    assert lineage_lines[2].startswith("    "), "Depth 2 should start with 4 spaces."

    for line in lineage_lines:
        assert re.search(r"\(parts=\d+\)", line)