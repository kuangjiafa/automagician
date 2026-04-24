import logging
from unittest.mock import patch

import pytest

from automagician.classes import Machine, SSHConfig
from automagician.machine import *


@pytest.mark.parametrize(
    "hostname,expected",
    [
        ("fri.cm.utexas.edu", Machine.FRI),
        ("halifax.cm.utexas.edu", Machine.HALIFAX),
        ("stampede2.tacc.utexas.edu", Machine.STAMPEDE2_TACC),
        ("frontera.tacc.utexas.edu", Machine.FRONTERA_TACC),
        ("ls6.tacc.utexas.edu", Machine.LS6_TACC),
        ("login1.fri.cm.utexas.edu", Machine.FRI),
        ("login2.halifax.cm.utexas.edu", Machine.HALIFAX),
        ("login3.stampede2.tacc.utexas.edu", Machine.STAMPEDE2_TACC),
        ("login0.frontera.tacc.utexas.edu", Machine.FRONTERA_TACC),
        ("login1.ls6.tacc.utexas.edu", Machine.LS6_TACC),
        ("unknown.host", Machine.UNKNOWN),
        ("login4.fri.cm.utexas.edu", Machine.UNKNOWN),  # Regex only matches 0-3
        ("login1.unknown.host", Machine.UNKNOWN),
    ],
)
@patch("socket.gethostname")
def test_get_machine_number(mock_gethostname, hostname, expected):
    mock_gethostname.return_value = hostname
    assert get_machine_number() == expected


def test_is_oden():
    assert is_oden(Machine.FRI)
    assert is_oden(Machine.HALIFAX)
    assert not is_oden(Machine.UNKNOWN)
    assert not is_oden(Machine.FRONTERA_TACC)
    assert not is_oden(Machine.LS6_TACC)
    assert not is_oden(Machine.STAMPEDE2_TACC)


def test_is_tacc():
    assert not is_tacc(Machine.FRI)
    assert not is_tacc(Machine.HALIFAX)
    assert not is_tacc(Machine.UNKNOWN)
    assert is_tacc(Machine.FRONTERA_TACC)
    assert is_tacc(Machine.LS6_TACC)
    assert is_tacc(Machine.STAMPEDE2_TACC)


def test_ssh_scp_init():
    logger = logging.Logger("test")
    ssh_config = ssh_scp_init(
        machine=get_machine_number(),
        home_dir=os.environ["HOME"],
        balance=True,
        logger=logger,
    )

    no_balance = ssh_scp_init(
        machine=get_machine_number(),
        home_dir=os.environ["HOME"],
        balance=False,
        logger=logger,
    )

    no_fabric = True
    wo_fabric = ssh_scp_init(
        machine=Machine.FRI, home_dir=os.environ["HOME"], balance=True, logger=logger
    )

    assert isinstance(ssh_config, SSHConfig)
    assert isinstance(no_balance, SSHConfig)
    assert isinstance(wo_fabric, SSHConfig)
    assert no_balance.config == "NoSSH"
    assert wo_fabric.config == "NoSSH"
