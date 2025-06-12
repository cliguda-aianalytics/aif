"""Testing secret_utils module."""

from aif.common.aif.src.utils.secret_utils import create_save_dict


def test_create_save_dict_simple():
    """Test create_save_dict for a simple dict."""
    simple_dict = {"a": 1, "b": 2, "c": 3}

    save_dict = create_save_dict(simple_dict, secret_keys=["b"])

    assert len(save_dict) == 3
    assert save_dict["a"] == 1
    assert save_dict["b"] == "--SECRET--"
    assert save_dict["c"] == 3


def test_create_save_dict_nested():
    """Test create_save_dict for a nested dict."""
    simple_dict = {
        "a": 1,
        "b": {"ba": "OK - ba", "bb": "DANGER"},
        "c": {"ca": "OK - ca", "cb": {"cba": "DANGER", "cbb": "OK - cbb"}},
    }

    save_dict = create_save_dict(simple_dict, secret_keys=["bb", "cba"])

    assert len(save_dict) == 3
    assert save_dict["a"] == 1

    assert len(save_dict["b"]) == 2
    assert save_dict["b"]["ba"] == "OK - ba"
    assert save_dict["b"]["bb"] == "--SECRET--"

    assert len(save_dict["c"]) == 2
    assert save_dict["c"]["ca"] == "OK - ca"

    assert len(save_dict["c"]["cb"]) == 2
    assert save_dict["c"]["cb"]["cba"] == "--SECRET--"
    assert save_dict["c"]["cb"]["cbb"] == "--SECRET--"  # bb is part of the key cbb
