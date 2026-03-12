import os

import automagician.constants as constants


def test_static_constants():
    assert constants.FRI_QUEUE_LIMIT == 5
    assert constants.HALIFAX_QUEUE_LIMIT == 10
    assert constants.TACC_QUEUE_LIMIT == 20
    assert constants.DEFAULT_SUBFILE_PATH_FRI_HALIFAX == "/home/sogetsoft/bin"
    assert constants.DEFAULT_SUBFILE_PATH_TACC == "/work/07194/hiimmorg/ls6/bin"
    assert constants.SORT_POS_PATH == "/home/kingraychardsarsenal/bin/sortpos.py"
    assert (
        constants.SO_GET_SOFT_PBE_PATH
        == "/home/kingraychardsarsenal/bin/sogetsoftpbe.py"
    )

    assert constants.DB_NAME == "automagician.db"
    assert constants.AUTOMAGIC_REMOTE_DIR == "/automagician_jobs"
    assert constants.DEFAULT_SUBFILE == "~/fri.sub"
    assert constants.PLAIN_TEXT_DB_NAME == "opt_jobs"
    assert constants.INVALID_DIR == "INVALID_DIR"
    assert constants.V_FIN_PL_PATH == "vfin.pl"
    assert constants.PRELIMINARY_RESULTS_NAME == "preliminary_results.dat"
    assert constants.CONVERGENCE_CERTIFICATE_NAME == "convergence_certificate"
    assert constants.TACC_QUEUE_MAXES == [50, 0, 200]


def test_dynamic_constants():
    user = os.environ.get("USER")
    expected_lock_dir = f"/tmp/automagician-{user}"
    assert constants.LOCK_DIR == expected_lock_dir
    assert constants.LOCK_FILE == os.path.join(expected_lock_dir, "lock")
