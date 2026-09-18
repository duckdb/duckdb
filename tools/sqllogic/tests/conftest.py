import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--unittest-binary",
        action="store",
        default=None,
        help="Provide the unittest binary to use for the tests",
    )


@pytest.fixture()
def unittest_binary(request):
    custom_arg = request.config.getoption("--unittest-binary")
    if not custom_arg:
        raise ValueError(
            "Please provide a unittest binary path to the tester, using '--unittest-binary <path_to_unittest>'"
        )
    return custom_arg
