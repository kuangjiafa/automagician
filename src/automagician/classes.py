from __future__ import annotations

import warnings
from dataclasses import dataclass
from enum import IntEnum
from typing import Literal, Union

try:
    from fabric import Connection, Transfer

    @dataclass
    class SshScp:
        ssh: Connection
        scp: Transfer

    @dataclass
    class SSHConfig:
        config: Union[Literal["NoSSH"], SshScp]

except ImportError:
    warnings.warn("Fabric was not imported; SSH/SCP features disabled", ImportWarning, stacklevel=1)
    Connection = None
    Transfer = None

    @dataclass
    class SSHConfig:
        config: Literal["NoSSH"]


class JobStatus(IntEnum):
    """An enum storing the status of a job

    Converged = 0
    Incomplete = 1
    Error = 2
    Running = -1
    NotFound = -10"""

    CONVERGED = 0
    INCOMPLETE = 1
    ERROR = 2
    RUNNING = -1
    NOT_FOUND = -10


class Machine(IntEnum):
    """Enum identifying the HPC cluster a job is running on.

    Values:
        FRI (0): fri.cm.utexas.edu
        HALIFAX (1): halifax.cm.utexas.edu
        STAMPEDE2_TACC (2): stampede2.tacc.utexas.edu
        FRONTERA_TACC (3): frontera.tacc.utexas.edu
        LS6_TACC (4): ls6.tacc.utexas.edu
        UNKNOWN (-1): hostname did not match any known cluster
    """

    FRI = 0
    HALIFAX = 1
    STAMPEDE2_TACC = 2
    FRONTERA_TACC = 3
    LS6_TACC = 4
    UNKNOWN = -1


@dataclass
class OptJob:
    """A class to represent an optimization job.

    Attributes:
        status: Current convergence/running status of the job.
        home_machine: The machine where this job was originally registered.
        last_on: The machine this job was most recently run on.
    """

    status: JobStatus
    home_machine: Machine
    last_on: Machine


@dataclass
class DosJob:
    """A density-of-states job, composed of an SC step followed by a DOS step.

    Attributes:
        opt_id: Row ID of the parent optimisation job in the database.
        sc_status: Status of the self-consistent (SC) charge-density calculation.
        dos_status: Status of the density-of-states calculation.
        sc_last_on: Machine the SC calculation was most recently run on.
        dos_last_on: Machine the DOS calculation was most recently run on.
    """

    opt_dir = None
    opt_id: Union[int, Literal[-1]]
    sc_status: JobStatus
    dos_status: JobStatus
    sc_last_on: Machine
    dos_last_on: Machine


@dataclass
class WavJob:
    """A job run specifically to obtain a WAVECAR file.

    Attributes:
        opt_id: Row ID of the parent optimisation job in the database.
        wav_status: Status of the WAVECAR calculation (see :class:`JobStatus`).
        wav_last_on: Machine the WAVECAR calculation was most recently run on.
    """

    opt_dir = None
    opt_id: Union[int, Literal[-1]]
    wav_status: JobStatus
    wav_last_on: Machine


@dataclass
class GoneJob:
    """A record of an opt job whose directory no longer exists on disk.

    Gone jobs are moved from ``opt_jobs`` into the ``gone_jobs`` database table
    by :func:`~automagician.process_job.gone_job_check`.

    Attributes:
        old_dir: The absolute path where the job used to live.
        status: The last known ``JobStatus`` before the directory disappeared.
        home_machine: The machine where the job was originally registered.
        last_on: The machine the job was most recently run on.
    """

    old_dir: str
    status: JobStatus
    home_machine: Machine
    last_on: Machine


class JobLimitError(Exception):
    """Raised when the pending submission queue would exceed the configured job limit.

    Caught by :func:`~automagician.main.main_wrapper` to halt further
    submissions while still allowing the ``finally`` block to flush the current
    queue and write database state.
    """

    def __init__(self) -> None:
        pass
