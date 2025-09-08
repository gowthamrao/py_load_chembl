import pytest

@pytest.fixture(scope='session')
def docker_compose_command():
    """Overrides the default `docker compose` command to use `sudo`."""
    return "sudo docker compose"
