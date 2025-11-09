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
    rdd1 = DummyRDD(num_of_parts=1)
    rdd2 = DummyRDD(parents=(rdd1,), num_of_parts=1)
    rdd3 = DummyRDD(parents=(rdd2,), num_of_parts=2)
    lineage_lines = rdd3.get_ascii_lineage().splitlines()

    assert lineage_lines[0].startswith(""), "Depth 0 should start with 0 spaces."
    assert lineage_lines[1].startswith("  "), "Depth 1 should start with 2 spaces."
    assert lineage_lines[2].startswith("    "), "Depth 2 should start with 4 spaces."

    for line in lineage_lines:
        assert re.search(r"\(parts=\d+\)", line)

def test_shared_tag_correctness():
    """ASCII lineage output shared tag applied for every repeated node encounter except first."""
    rdd1 = DummyRDD()
    rdd2 = DummyRDD(parents=(rdd1,))
    rdd3 = DummyRDD(parents=(rdd1,))
    rdd4 = DummyRDD(parents=(rdd1,))
    rdd5 = DummyRDD(parents=(rdd2, rdd3, rdd4))
    lineage_lines = rdd5.get_ascii_lineage()

    assert lineage_lines.count("(shared)") == 2, "Incorrect shared node tagging logic."

def test_edges_output_determinism():
    """Lineage edges output list must produce deterministic output."""
    rdd1 = DummyRDD()
    rdd2 = DummyRDD()
    rdd3 = DummyRDD(parents=(rdd1, rdd2))
    lineage_edges_1 = rdd3.get_lineage_edges()
    lineage_edges_2 = rdd3.get_lineage_edges()

    assert lineage_edges_1 == lineage_edges_2, "Lineage edges order must remain deterministic across calls."