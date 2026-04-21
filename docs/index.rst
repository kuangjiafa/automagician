automagician
============

**automagician** automates the submission and lifecycle management of `VASP
<https://www.vasp.at/>`_ geometry optimisation calculations on HPC clusters
(FRI, Halifax, Stampede2, Frontera, LS6).

It tracks every job in a local SQLite database, detects convergence, fixes
common VASP errors, spawns follow-on DOS/WAV calculations, and — when
configured — balances load across two machines via SSH/SCP.

.. toctree::
   :maxdepth: 2
   :caption: Getting started

   usage/quickstart

.. toctree::
   :maxdepth: 2
   :caption: API reference

   api/index

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
