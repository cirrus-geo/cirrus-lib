import pytest

from cirrus.lib import utils



same_dicts = [
    ({'a': 1, 'b': 2},
     {'b': 2, 'a': 1}),
    ({'a': 1, 'b': 2},
     {'a': 1, 'b': 2}),
    ({'a': 1, 'b': [1,2,3]},
     {'b': [1,2,3], 'a': 1}),
]


@pytest.mark.parametrize('dicts', same_dicts)
def test_recursive_compare_same(dicts):
    assert utils.recursive_compare(dicts[0], dicts[1])


diff_dicts = [
    ({'a': 1, 'b': 2},
     {'b': 1, 'a': 2}),
    ({'a': 1, 'b': 2},
     {'a': 2, 'b': 1}),
    ({'a': 1, 'b': [1,2,3]},
     {'a': 1, 'b': [1]}),
]


@pytest.mark.parametrize('dicts', diff_dicts)
def test_recursive_compare_diff(dicts):
    assert not utils.recursive_compare(dicts[0], dicts[1])
