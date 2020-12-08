import sys
from nose.tools import assert_true, assert_false, assert_equal, nottest
try:
    from unittest.mock import MagicMock, Mock, patch
except ImportError:
    from mock import MagicMock, Mock, patch

from yaml import load as yaml_load

def test_get_mmsid_from_bag_info():
    with open("tests/bag-info.txt", "r") as f:
        bag_info = yaml_load(f.read())
    mmsid = bag_info['FIELD_EXTERNAL_DESCRIPTION'].split()[-1].strip()
    assert_equal(mmsid, "9910306702042")
