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


def test_get_subfile():
    assert get_subfile(Machine.FRI) == "fri.sub"
    assert get_subfile(Machine.HALIFAX) == "halifax.sub"
    assert get_subfile(Machine.STAMPEDE2_TACC) == "knl.mpi.slurm"
    assert get_subfile(Machine.FRONTERA_TACC) == "clx.mpi.slurm"
    assert get_subfile(Machine.LS6_TACC) == "milan.mpi.slurm"
    assert get_subfile(Machine.UNKNOWN) == "INVALID"
    assert get_subfile(None) == "INVALID"
