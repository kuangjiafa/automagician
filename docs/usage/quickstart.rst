Quickstart
==========

Installation
------------

1. **Create and activate a virtual environment**:

   .. code-block:: bash

      ./build.sh create
      source .venv/bin/activate

2. **Build and install the package**:

   .. code-block:: bash

      ./build.sh build
      python -m pip install dist/automagician-*.whl

   Alternatively, install in editable (development) mode:

   .. code-block:: bash

      ./build.sh install_dev

3. **Install the optional SSH/SCP support** (needed for FRI–Halifax load
   balancing):

   .. code-block:: bash

      pip install "automagician[remote]"

Running automagician
--------------------

From the directory that contains your VASP job folders, run:

.. code-block:: bash

   automagician

Pass ``--help`` to see all available options:

.. code-block:: bash

   automagician --help

Key command-line options
------------------------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Flag
     - Description
   * - ``--limit N``
     - Maximum number of jobs to keep queued at once (default: unlimited).
   * - ``--balance``
     - Enable load-balancing between FRI and Halifax via SSH/SCP.
   * - ``--clear-certificate``
     - Ignore existing convergence certificates and re-evaluate convergence.
   * - ``--db PATH``
     - Path to the SQLite database file (default: ``automagician.db`` in the
       current directory).

Supported machines
------------------

automagician detects the current host automatically:

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Machine
     - Hostname
     - Submission script
   * - FRI
     - ``fri.cm.utexas.edu``
     - ``fri.sub``
   * - Halifax
     - ``halifax.cm.utexas.edu``
     - ``halifax.sub``
   * - Stampede2
     - ``stampede2.tacc.utexas.edu``
     - ``knl.mpi.slurm``
   * - Frontera
     - ``frontera.tacc.utexas.edu``
     - ``clx.mpi.slurm``
   * - LS6
     - ``ls6.tacc.utexas.edu``
     - ``milan.mpi.slurm``

How it works
------------

Each invocation of automagician performs the following steps:

1. **Register** — scans the filesystem for VASP job directories and adds new
   ones to the SQLite database.
2. **Get submitted jobs** — queries ``squeue`` (and ``sacct`` for FRI/Halifax)
   to determine which jobs are currently running.
3. **Process opt jobs** — for each known optimisation job, checks convergence,
   wraps up completed runs with ``vfin.pl``, and re-queues incomplete jobs.
4. **Process DOS/WAV jobs** — once an optimisation job converges, creates and
   submits the corresponding density-of-states (SC + DOS) or WAVECAR
   calculations.
5. **Check gone jobs** — marks directories that have disappeared from the
   filesystem as "gone" so they are excluded from future runs.
6. **Submit queue** — submits all pending jobs to the scheduler, balancing
   across machines if requested.
7. **Write database** — persists all status updates back to the SQLite file.

Building the documentation
--------------------------

Install the documentation dependencies, then run Sphinx:

.. code-block:: bash

   pip install "automagician[docs]"
   cd docs
   make html

The built HTML will be in ``docs/_build/html/index.html``.
