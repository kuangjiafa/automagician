import logging

from automagician.classes import Machine, SSHConfig
from automagician.machine import *


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


def test_get_machine_name():
    assert get_machine_name(Machine.FRI) == "fri.cm.utexas.edu"
    assert get_machine_name(Machine.HALIFAX) == "halifax.cm.utexas.edu"
    assert get_machine_name(Machine.STAMPEDE2_TACC) == "stampede2.tacc.utexas.edu"
    assert get_machine_name(Machine.FRONTERA_TACC) == "frontera.tacc.utexas.edu"
    assert get_machine_name(Machine.LS6_TACC) == "ls6.tacc.utexas.edu"
    assert get_machine_name(Machine.UNKNOWN) == "localhost"
    assert get_machine_name(999) == "localhost"


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
