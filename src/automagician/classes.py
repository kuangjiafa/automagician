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
    print("Fabric was not imported.")
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
    FRI = 0
    HALIFAX = 1
    STAMPEDE2_TACC = 2
    FRONTERA_TACC = 3
    LS6_TACC = 4
    UNKNOWN = -1


@dataclass
class OptJob:
    """A class to represent an optimization job

    status
    home_machine
      The machine this job is on
    last_on
      The machine this job was last ran on
    """

    status: JobStatus
    home_machine: Machine
    last_on: Machine


@dataclass
class DosJob:
    """Density optimization job

    opt_id
      The id of the optimization job this is connected to
    sc_status
      The status of the optimization job this is connected to
    dos_status
        The status of the job
    sc_last_on
      The machine the of the optimization job this is connected to
    dos_last_on
      The machine that this job was connected to
    """

    opt_dir = None
    opt_id: int | Literal[-1]
    sc_status: JobStatus
    dos_status: JobStatus
    sc_last_on: Machine
    dos_last_on: Machine


@dataclass
class WavJob:
    """A job ran specifically to obtain a WAVECAR
    opt_id
      The id of the optimization job this is connected to
    wav_status
      The status of the job
      0 = done
      1 = nonexistent or unconverged (Unclear) TODO: Get what this means
      2 = error
      -1 = running
    wav_last_on
      The machine that this job was connected to"""

    opt_dir = None
    opt_id: int | Literal[-1]
    wav_status: JobStatus
    wav_last_on: Machine


@dataclass
class GoneJob:
    """A record of jobs that can no longer be found"""

    old_dir: str
    status: JobStatus
    home_machine: Machine
    last_on: Machine


class JobLimitError(Exception):
    """What happens if you submit too many jobs"""

    def __init__(self) -> None:
        pass
