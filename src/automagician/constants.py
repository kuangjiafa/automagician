import os

LOCK_FILE = f"/tmp/automagician/{os.environ['USER']}-lock"
LOCK_DIR = "/tmp/automagician"
DB_NAME = "automagician.db"
AUTOMAGIC_REMOTE_DIR = "/automagician_jobs"
DEFAULT_SUBFILE = "~/fri.sub"
DEFAULT_SUBFILE_PATH_FRI_HALIFAX = (
    "/home/kg33564/automagician-permanent/subfile-archive"
)
PLAIN_TEXT_DB_NAME = "opt_jobs"
DEFAULT_SUBFILE_PATH_TACC = "/work2/08734/karan/automagician-slurm-templates"
INVALID_DIR = "INVALID_DIR"
SORT_POS_PATH = "/home/wc5879/kingRaychardsArsenal/sortpos.py"
SO_GET_SOFT_PBE_PATH = "/home/wc5879/kingRaychardsArsenal/sogetsoftpbe.py"
# V_FIN_PL_PATH = "/opt/ohpc/pub/libs/vtstscripts/1033/vfin.pl"
V_FIN_PL_PATH = "vfin.pl"
PRELIMINARY_RESULTS_NAME = "preliminary_results.dat"
CONVERGENCE_CERTIFICATE_NAME = "convergence_certificate"
TACC_QUEUE_MAXES = [
    50,
    0,
    200,
]  # stampede2 knl normal, frontera normal, ls6 normal respectively # no frontera allocation -> alloc 0
