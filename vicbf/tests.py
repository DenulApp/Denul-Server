"""Test cases for the VICBF implementation.

These test cases are run with "nosetests".
"""

from vicbf import VICBF, deserialize

"""Constructor tests"""


def test_incorrect_constructor_slots():
    try:
        VICBF(-1, 3)
    except ValueError:
        assert True
        return
    assert False


def test_incorrect_constructor_hashfunctions():
    try:
        VICBF(1000, -1)
    except ValueError:
        assert True
        return
    assert False


def test_incorrect_constructor_vibase():
    try:
        VICBF(1000, 3, vibase=3)
    except ValueError:
        assert True
        return
    assert False

"""Insertion tests"""


def test_insert():
    v = VICBF(10000, 3)
    v.insert(123)
    assert True


def test_insert_none():
    v = VICBF(10000, 3)
    try:
        v.insert(None)
    except ValueError:
        assert True
        return
    assert False


def test_many_inserts():
    v = VICBF(10000, 3)
    for i in range(1000):
        v.insert(i)
    assert not v.query(1001)


def test_insert_overflow():
    v = VICBF(10000, 3)
    for i in range(1000):
        v.insert(123)
    assert v.query(123)
    for i in range(1000):
        v.remove(123)
    # Even though it should now theoretically be removed, the implementation
    # should still return true because the values should have been fixed at
    # the maximum when the overflow occured
    assert v.query(123)


def test_insert_list_syntax():
    v = VICBF(10000, 3)
    v += 123
    assert 123 in v

"""Query tests"""


def test_query_inserted():
    v = VICBF(10000, 3)
    v.insert(123)
    assert v.query(123)


def test_query_not_inserted():
    v = VICBF(10000, 3)
    v.insert(123)
    assert not v.query(4567)


def test_query_multi_insert_remove():
    v = VICBF(10000, 3)
    v.insert(123)
    v.insert(123)
    v.remove(123)
    assert v.query(123)


def test_query_none():
    v = VICBF(10000, 3)
    try:
        v.query(None)
    except ValueError:
        assert True
        return
    assert False


def test_query_list_syntax():
    v = VICBF(10000, 3)
    v.insert(123)
    assert 123 in v
    assert 124 not in v


"""Removal tests"""


def test_remove():
    v = VICBF(10000, 3)
    v.insert(123)
    v.remove(123)
    assert not v.query(123)


def test_remove_not_inserted():
    v = VICBF(10000, 3)
    try:
        v.remove(124)
    except ValueError:
        assert True
        return
    assert False


def test_remove_none():
    v = VICBF(10000, 3)
    try:
        v.remove(None)
    except ValueError:
        assert True
        return
    assert False


def test_remove_list_syntax():
    v = VICBF(10000, 3)
    v += 123
    v += 124
    v -= 123
    assert 123 not in v
    assert 124 in v


"""Helper function tests"""


def test_fpr_helper():
    # Test the FPR calculator with a few known-good values, calculated with
    # WolframAlpha
    v = VICBF(10000, 3)
    fpr = v._calculate_FPR(10000, 1000, 3, 4)
    assert abs(fpr - 0.00066503041161) <= 0.00000000000001
    fpr = v._calculate_FPR(5000, 5000, 3, 4)
    assert abs(fpr - 0.51818886904) <= 0.00000000001
    fpr = v._calculate_FPR(5000, 5000, 3, 8)
    assert abs(fpr - 0.47966585318) <= 0.00000000001
    fpr = v._calculate_FPR(5000, 5000, 2, 4)
    assert abs(fpr - 0.38364688995) <= 0.00000000001


def test_current_fpr():
    v = VICBF(10000, 3)
    for i in range(1000):
        v += i
    assert abs(v.FPR() - 0.00066503041161) <= 0.00000000000001


def test_size():
    v = VICBF(10000, 3)
    v += 123
    v += 124
    assert v.size() == 2
    v -= 124
    v -= 123
    assert v.size() == 0
    try:
        v -= 123
    except Exception:
        pass
    assert v.size() == 0


def test_len():
    v = VICBF(10000, 3)
    v += 123
    v += 124
    assert len(v) == 2
    v -= 124
    v -= 123
    assert len(v) == 0
    try:
        v -= 123
    except Exception:
        pass
    assert len(v) == 0


def test_serialization():
    v = VICBF(10000, 3)
    for i in range(5000):
        v += i
    ser = v.serialize()
    v2 = deserialize(ser)
    for i in range(5000):
        assert i in v2


def test_serialization2():
    v = VICBF(10000, 3)
    v += 123
    v += 126
    ser = v.serialize()
    v2 = deserialize(ser)
    assert 123 in v2
    assert 126 in v2
    assert 124 not in v2

test_serialization()
