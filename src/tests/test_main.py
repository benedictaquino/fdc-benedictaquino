from fig_data_challenge.main import return_42


def test_main():
    value = return_42()
    assert value == 42
