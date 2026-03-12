import os

FRI_QUEUE_LIMIT = 5
HALIFAX_QUEUE_LIMIT = 10
TACC_QUEUE_LIMIT = 20

DEFAULT_SUBFILE_PATH_FRI_HALIFAX = "/home/sogetsoft/bin"
DEFAULT_SUBFILE_PATH_TACC = "/work/07194/hiimmorg/ls6/bin"

SORT_POS_PATH = "/home/kingraychardsarsenal/bin/sortpos.py"
SO_GET_SOFT_PBE_PATH = "/home/kingraychardsarsenal/bin/sogetsoftpbe.py"

LOCK_DIR = f"/tmp/automagician-{os.environ['USER']}"
LOCK_FILE = os.path.join(LOCK_DIR, "lock")
DB_NAME = "automagician.db"
AUTOMAGIC_REMOTE_DIR = "/automagician_jobs"
DEFAULT_SUBFILE = "~/fri.sub"
PLAIN_TEXT_DB_NAME = "opt_jobs"
INVALID_DIR = "INVALID_DIR"

# V_FIN_PL_PATH = "/opt/ohpc/pub/libs/vtstscripts/1033/vfin.pl"
V_FIN_PL_PATH = "vfin.pl"
PRELIMINARY_RESULTS_NAME = "preliminary_results.dat"
CONVERGENCE_CERTIFICATE_NAME = "convergence_certificate"
TACC_QUEUE_MAXES = [
    50,
    0,
    200,
]  # stampede2 knl normal, frontera normal, ls6 normal respectively # no frontera allocation -> alloc 0
