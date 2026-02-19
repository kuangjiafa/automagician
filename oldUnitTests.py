#!/usr/bin/env python

import os
import shutil
import sqlite3
import automagician


test_file_dir = "/home/jw53959/project/test_files"


def test_check_has_opt(directory: str):
    """Tests check_has_opt
    
    directory - A path to the directory used for testing. Is a string
    
    prints the tests_failed to standard output"""
    # setup
    total_tests = 0
    tests_failed = 0
    individual_tests_failed = []
    test_dir = directory + "check_has_opt/"

    os.mkdir(test_dir)

    automagician.subfile = "fri.sub"
    files = os.listdir(test_dir)
    if automagician.check_has_opt(files):
       tests_failed += 1
       individual_tests_failed.append["empty directory"]
    total_tests += 1

    file = open(test_dir + "POSCAR", "w")
    file.close()
    files = os.listdir(test_dir)
    if automagician.check_has_opt(files):
       tests_failed += 1
       individual_tests_failed.append["only POSCAR"]
    total_tests += 1
    os.remove(test_dir + "POSCAR")

    file = open(test_dir + "POTCAR", "w")
    file.close()
    files = os.listdir(test_dir)
    if automagician.check_has_opt(files):
       tests_failed += 1
       individual_tests_failed.append["only POTCAR"]
    total_tests += 1
    os.remove(test_dir + "POTCAR")

    file = open(test_dir + "INCAR", "w")
    file.close()
    files = os.listdir(test_dir)
    if automagician.check_has_opt(files):
       tests_failed += 1
       individual_tests_failed.append["only INCAR"]
    total_tests += 1
    os.remove(test_dir + "INCAR")

    file = open(test_dir + "junk", "w")
    file.close()
    files = os.listdir(test_dir)
    if automagician.check_has_opt(files):
       tests_failed += 1
       individual_tests_failed.append["only junk"]
    total_tests += 1
    os.remove(test_dir + "junk")

    file = open(test_dir + "KPOINTS", "w")
    file.close()
    files = os.listdir(test_dir)
    if automagician.check_has_opt(files):
       tests_failed += 1
       individual_tests_failed.append["only KPOINTS"]
    total_tests += 1
    os.remove(test_dir + "KPOINTS")

    file = open(test_dir + "fri.sub", "w")
    file.close()
    files = os.listdir(test_dir)
    if automagician.check_has_opt(files):
       tests_failed += 1
       individual_tests_failed.append["only fri.sub"]
    total_tests += 1
    os.remove(test_dir + "fri.sub")


    file = open(test_dir + "KPOINTS", "w")
    file.close()
    file = open(test_dir + "fri.sub", "w")
    file.close()
    file = open(test_dir + "INCAR", "w")
    file.close()
    file = open(test_dir + "POSCAR", "w")
    file.close()
    file = open(test_dir + "POTCAR", "w")
    file.close()
    files = os.listdir(test_dir)
    if not automagician.check_has_opt(files):
       tests_failed += 1
       individual_tests_failed.append("all files present")
    total_tests += 1
    os.remove(test_dir + "KPOINTS")
    os.remove(test_dir + "fri.sub")
    os.remove(test_dir + "INCAR")
    os.remove(test_dir + "POSCAR")
    os.remove(test_dir + "POTCAR")


    file = open(test_dir + "junk1", "w")
    file.close()
    file = open(test_dir + "junk2", "w")
    file.close()
    file = open(test_dir + "junk3", "w")
    file.close()
    file = open(test_dir + "junk4", "w")
    file.close()
    file = open(test_dir + "junk5", "w")
    file.close()
    files = os.listdir(test_dir)
    if automagician.check_has_opt(files):
       tests_failed += 1
       individual_tests_failed.append("5 junk present")
    total_tests += 1
    os.remove(test_dir + "junk1")
    os.remove(test_dir + "junk2")
    os.remove(test_dir + "junk3")
    os.remove(test_dir + "junk4")
    os.remove(test_dir + "junk5")


    file = open(test_dir + "KPOINTS", "w")
    file.close()
    file = open(test_dir + "fri.sub", "w")
    file.close()
    file = open(test_dir + "INCAR", "w")
    file.close()
    file = open(test_dir + "POSCAR", "w")
    file.close()
    file = open(test_dir + "POTCAR", "w")
    file.close()
    file = open(test_dir + "junk1", "w")
    file.close()
    files = os.listdir(test_dir)
    if not automagician.check_has_opt(files):
       tests_failed += 1
       individual_tests_failed.append("all files present and 1 junk")
    total_tests += 1
    os.remove(test_dir + "KPOINTS")
    os.remove(test_dir + "fri.sub")
    os.remove(test_dir + "INCAR")
    os.remove(test_dir + "POSCAR")
    os.remove(test_dir + "POTCAR")
    os.remove(test_dir + "junk1")

    file = open(test_dir + "KPOINTS", "w")
    file.close()
    file = open(test_dir + "frilab.sub", "w")
    file.close()
    file = open(test_dir + "INCAR", "w")
    file.close()
    file = open(test_dir + "POSCAR", "w")
    file.close()
    file = open(test_dir + "POTCAR", "w")
    file.close()
    files = os.listdir(test_dir)
    if automagician.check_has_opt(files):
       tests_failed += 1
       individual_tests_failed.append("Subfile incorrect match (frilab.sub instead of fri.sub)")
    total_tests += 1
    os.remove(test_dir + "KPOINTS")
    os.remove(test_dir + "frilab.sub")
    os.remove(test_dir + "INCAR")
    os.remove(test_dir + "POSCAR")
    os.remove(test_dir + "POTCAR")

    file = open(test_dir + "fri.sub", "w")
    file.close()
    file = open(test_dir + "INCAR", "w")
    file.close()
    file = open(test_dir + "POSCAR", "w")
    file.close()
    file = open(test_dir + "POTCAR", "w")
    file.close()
    files = os.listdir(test_dir)
    if automagician.check_has_opt(files):
       tests_failed += 1
       individual_tests_failed.append("Missing Kpoints")
    total_tests += 1
    os.remove(test_dir + "fri.sub")
    os.remove(test_dir + "INCAR")
    os.remove(test_dir + "POSCAR")
    os.remove(test_dir + "POTCAR")

    file = open(test_dir + "KPOINTS", "w")
    file.close()
    file = open(test_dir + "INCAR", "w")
    file.close()
    file = open(test_dir + "POSCAR", "w")
    file.close()
    file = open(test_dir + "POTCAR", "w")
    file.close()
    files = os.listdir(test_dir)
    if automagician.check_has_opt(files):
       tests_failed += 1
       individual_tests_failed.append("missing subfile")
    total_tests += 1
    os.remove(test_dir + "KPOINTS")
    os.remove(test_dir + "INCAR")
    os.remove(test_dir + "POSCAR")
    os.remove(test_dir + "POTCAR")

    file = open(test_dir + "KPOINTS", "w")
    file.close()
    file = open(test_dir + "fri.sub", "w")
    file.close()
    file = open(test_dir + "POSCAR", "w")
    file.close()
    file = open(test_dir + "POTCAR", "w")
    file.close()
    files = os.listdir(test_dir)
    if automagician.check_has_opt(files):
       tests_failed += 1
       individual_tests_failed.append("missing INCAR")
    total_tests += 1
    os.remove(test_dir + "KPOINTS")
    os.remove(test_dir + "fri.sub")
    os.remove(test_dir + "POSCAR")
    os.remove(test_dir + "POTCAR")

    file = open(test_dir + "KPOINTS", "w")
    file.close()
    file = open(test_dir + "fri.sub", "w")
    file.close()
    file = open(test_dir + "INCAR", "w")
    file.close()
    file = open(test_dir + "POTCAR", "w")
    file.close()
    files = os.listdir(test_dir)
    if automagician.check_has_opt(files):
       tests_failed += 1
       individual_tests_failed.append("missing POSCAR")
    total_tests += 1
    os.remove(test_dir + "KPOINTS")
    os.remove(test_dir + "fri.sub")
    os.remove(test_dir + "INCAR")
    os.remove(test_dir + "POTCAR")


    file = open(test_dir + "KPOINTS", "w")
    file.close()
    file = open(test_dir + "fri.sub", "w")
    file.close()
    file = open(test_dir + "INCAR", "w")
    file.close()
    file = open(test_dir + "POSCAR", "w")
    file.close()
    files = os.listdir(test_dir)
    if automagician.check_has_opt(files):
       tests_failed += 1
       individual_tests_failed.append("missing POTCAR")
    total_tests += 1
    os.remove(test_dir + "KPOINTS")
    os.remove(test_dir + "fri.sub")
    os.remove(test_dir + "INCAR")
    os.remove(test_dir + "POSCAR")



    print_test_results(total_tests, tests_failed, "check_has_opt", individual_tests_failed)

def new_test_has_opt():
   total_tests = 0
   tests_failed = 0
   detailed_tests_failed = []

   max = 2**7
   subfile = "fri.sub"
   automagician.subfile = subfile
   for i in range(max):
      has_junk1 = i & 1
      has_subfile = i & 2
      has_INCAR = i & 4
      has_POSCAR = i & 8
      has_KPOINTS = i & 16
      has_POTCAR = i & 32
      has_wrong_subfile = i & 64
      files = []
      if has_junk1:
         files.append("junk1")
      if has_subfile:
         files.append(subfile)
      if has_INCAR:
         files.append("INCAR")
      if has_POSCAR:
         files.append("POSCAR")
      if has_KPOINTS:
         files.append("KPOINTS")
      if has_POTCAR:
         files.append("POTCAR")
      if has_wrong_subfile:
         files.append("halifax.sub")
      total_tests += 1
      if has_subfile and has_INCAR and has_POSCAR and has_POTCAR and has_KPOINTS and has_subfile:
         if not automagician.check_has_opt(files):
            tests_failed += 1
            fail_string = """Check_has_opt returned false when has_junk1 = {}, has_subfile = {}, has_INCAR = {}, has_POSCAR = {}, has_POTCAR = {}, has_KPOINTS = {}, has_wrong_subfile = {}""".format(
               has_junk1, has_subfile, has_INCAR, has_POSCAR, has_POTCAR, has_KPOINTS, has_wrong_subfile)
            detailed_tests_failed.append(fail_string)
      else:
         if automagician.check_has_opt(files):
            tests_failed += 1
            fail_string = """Check_has_opt returned true when has_junk1 = {}, has_subfile = {}, has_INCAR = {}, has_POSCAR = {}, has_POTCAR = {}, has_KPOINTS = {}, has_wrong_subfile = {}""".format(
               has_junk1, has_subfile, has_INCAR, has_POSCAR, has_POTCAR, has_KPOINTS, has_wrong_subfile)
            detailed_tests_failed.append(fail_string)
   print_test_results(total_tests, tests_failed, "check_has_opt_detailed", detailed_tests_failed)

def test_db_init(directory:str):
   total_tests = 0
   tests_failed = 0
   detailed_tests_failed = []
   database = directory + "test_database"

   automagician.db_init(database)
   names = automagician.db.execute("select name from sqlite_master where type='table'")
   total_tests += 1
   if not check_db_tables(names):
      tests_failed += 1
      detailed_tests_failed.append("Empty database")
   
   # Test to see if something was present if it gets overwriten
   automagician.db.execute("INSERT into opt_jobs values (?,?,?,?)", ("\\tmp", automagician.JobStatus.Converged.value,0,0))
   automagician.db.connection.commit()
   automagician.db_init(database)
   names = automagician.db.execute("select name from sqlite_master where type='table'")
   total_tests += 1
   if not check_db_tables(names):
      tests_failed += 1
      detailed_tests_failed.append("Full database")
   
   opt_jobs = automagician.db.execute("SELECT * from opt_jobs").fetchall()
   total_tests += 1
   if len(opt_jobs) != 1:
      tests_failed += 1
      detailed_tests_failed.append("Data was lost or added in db_init")
   # Test to see that data was the same
   job = opt_jobs[0]
   total_tests += 1
   if job[0] != "\\tmp":
      tests_failed += 1
      detailed_tests_failed.append("job directory corrupted in db_init")
   total_tests += 1
   if job[1] != automagician.JobStatus.Converged.value:
      tests_failed += 1
      detailed_tests_failed.append("Job status corrupted in db_init")
   total_tests += 1
   if job[2] != 0:
      tests_failed += 1
      detailed_tests_failed.append("Home macine corrupted in db_init")
   total_tests += 1
   if job[3] != 0:
      tests_failed += 1
      detailed_tests_failed.append("Last on corrupted in db_init")

   automagician.db.execute("DROP TABLE dos_jobs")
   automagician.db_init(database)
   names = automagician.db.execute("select name from sqlite_master where type='table'")
   total_tests += 1
   if not check_db_tables(names):
      tests_failed += 1
      detailed_tests_failed.append("Database missing dos_jobs")
   
   opt_jobs = automagician.db.execute("SELECT * from opt_jobs").fetchall()
   total_tests += 1
   if len(opt_jobs) != 1:
      tests_failed += 1
      detailed_tests_failed.append("Data was lost or added in db_init")
   # Test to see that data was the same
   job = opt_jobs[0]
   total_tests += 1
   if job[0] != "\\tmp":
      tests_failed += 1
      detailed_tests_failed.append("job directory corrupted in db_init")
   total_tests += 1
   if job[1] != automagician.JobStatus.Converged.value:
      tests_failed += 1
      detailed_tests_failed.append("Job status corrupted in db_init")
   total_tests += 1
   if job[2] != 0:
      tests_failed += 1
      detailed_tests_failed.append("Home macine corrupted in db_init")
   total_tests += 1
   if job[3] != 0:
      tests_failed += 1
      detailed_tests_failed.append("Last on corrupted in db_init")
   automagician.db.connection.commit()

   print_test_results(total_tests, tests_failed, "db_init", detailed_tests_failed)
   os.remove(database)

def check_db_tables(names):
   tables = 0
   for name in names:
      trimmed_name = name[0]
      if trimmed_name == "opt_jobs":
         tables |= 1
      elif trimmed_name == "dos_jobs":
         tables |= 2
      elif trimmed_name == "wav_jobs":
         tables |= 4
      elif trimmed_name == "gone_jobs":
         tables |= 8
      elif trimmed_name == "insta_submit":
         tables |= 16
      else: 
         tables |= 32
   return tables == 31

def test_del_pwd(directory:str):
   # Setup
   total_tests = 0
   tests_failed = 0
   detailed_tests_failed = []
   database = directory + "test_database"
   automagician.db_init(database)
   old_working_directory = os.getcwd()
   os.mkdir(directory + "test1/")
   os.mkdir(directory + "test2/")
   os.mkdir(directory + "test3/")
   # Test
   automagician.db.execute("INSERT into opt_jobs values (?,?,?,?)", 
                           (os.getcwd(), automagician.JobStatus.Converged.value,3,5))
   automagician.db.connection.commit()
   automagician.delpwd()
   opt_jobs = automagician.db.execute("SELECT * from opt_jobs").fetchall()
   total_tests += 1
   if len(opt_jobs) != 0:
      tests_failed += 1
      detailed_tests_failed.append("Did not delete last object")
   

   automagician.db.execute("INSERT into opt_jobs values (?,?,?,?)", 
                           (os.getcwd(), automagician.JobStatus.Converged.value,3,5))
   automagician.db.connection.commit()
   os.chdir(directory + "test1/")
   automagician.delpwd()
   opt_jobs = automagician.db.execute("SELECT * from opt_jobs").fetchall()
   total_tests += 1
   if len(opt_jobs) != 1:
      tests_failed += 1
      detailed_tests_failed.append("Deleted object that shouldnt have been")
   total_tests += 1
   job = opt_jobs[0]
   if job[0] != old_working_directory:
      tests_failed += 1
      detailed_tests_failed.append("job directory corrupted in delpwd")
   total_tests += 1
   if job[1] != automagician.JobStatus.Converged.value:
      tests_failed += 1
      detailed_tests_failed.append("Job status corrupted in delpwd")
   total_tests += 1
   if job[2] != 3:
      tests_failed += 1
      detailed_tests_failed.append("Home macine corrupted in delpwd")
   total_tests += 1
   if job[3] != 5:
      tests_failed += 1
      detailed_tests_failed.append("Last on corrupted in delpwd")



   print_test_results(total_tests, tests_failed, "delpwd", detailed_tests_failed)
   # Teardown
   os.chdir(old_working_directory)
   os.remove(database)
   os.rmdir(directory + "test1/")
   os.rmdir(directory + "test2/")
   os.rmdir(directory + "test3/")

def test_determine_convergence(directory:str):
   
   # Setup
   total_tests = 0
   tests_failed = 0
   detailed_tests_failed = []

   # Tests

   # Standard optomization
   os.mkdir(directory + "test1/")
   result = automagician.determine_convergence(directory+"test1")
   total_tests += 1
   if result:
      tests_failed += 1
      detailed_tests_failed.append("An empty directory was considerd converged")

   file = open(directory + "test1/convergence_certificate", "w")
   result = automagician.determine_convergence(directory+"test1/")
   total_tests += 1
   if not result:
      tests_failed += 1
      detailed_tests_failed.append("A directory with only a convergence certiciate was not considered converged")
   file.close()
   os.remove(directory + "test1/convergence_certificate")

   file = open(directory + "test1/CONTCAR", "w")
   file.close()
   result = automagician.determine_convergence(directory+"test1")
   total_tests += 1
   if result:
      tests_failed += 1
      detailed_tests_failed.append("A directory with only a CONTCAR was considered converged")
   os.remove(directory + "test1/CONTCAR")

   file = open(directory + "test1/ll_out", "w")
   file.close()
   result = automagician.determine_convergence(directory+"test1/")
   total_tests += 1
   if result:
      tests_failed += 1
      detailed_tests_failed.append("A directory with only a ll_out was considered converged")
   os.remove(directory + "test1/ll_out")

   file = open(directory + "test1/ll_out", "w")
   file.close()
   file = open(directory + "test1/CONTCAR", "w")
   file.close()
   result = automagician.determine_convergence(directory+"test1")
   total_tests += 1
   if result:
      tests_failed += 1
      detailed_tests_failed.append("A directory with empty ll_out and CONTCAR was considered converged")
   os.remove(directory + "test1/ll_out")
   os.remove(directory + "test1/CONTCAR")
   if os.path.exists(directory + "test1/fe.dat"):
      os.remove(directory + "test1/fe.dat")
   

   file = open(directory + "test1/ll_out", "w")
   file.write("reached required accuracy - stopping structural energy minimisation")
   file.close()
   file = open(directory + "test1/CONTCAR", "w")
   file.close()
   file = open(directory + "test1/INCAR", "w")
   file.close()
   result = automagician.determine_convergence(directory+"test1")
   total_tests += 1
   if not result:
      tests_failed += 1
      detailed_tests_failed.append("A directory with ll_out that said converges was not considered converged")
   os.remove(directory + "test1/ll_out")
   os.remove(directory + "test1/CONTCAR")
   os.remove(directory + "test1/INCAR")
   if os.path.exists(directory + "test1/fe.dat"):
      os.remove(directory + "test1/fe.dat")
   


   # Box
   file = open(directory + "test1/ll_out", "w")
   file.write("reached required accuracy - stopping structural energy minimisation")
   file.close()
   file = open(directory + "test1/CONTCAR", "w")
   file.close()
   file = open(directory + "test1/INCAR", "w")
   file.write("ISIF = 3")
   file.close()
   result = automagician.determine_convergence(directory+"test1")
   total_tests += 1
   if  result:
      tests_failed += 1
      detailed_tests_failed.append("A directory with a convergent ll_out, but ISIF = 3 was considered converged")
   os.remove(directory + "test1/ll_out")
   os.remove(directory + "test1/CONTCAR")
   os.remove(directory + "test1/INCAR")
   if os.path.exists(directory + "test1/fe.dat"):
      os.remove(directory + "test1/fe.dat")
   os.rmdir(directory + "test1/")

   # Teardown
   print_test_results(total_tests, tests_failed, "determine_convergence", detailed_tests_failed)

def test_determine_is_isif3(directory:str):
   # Setup
   total_tests = 0
   tests_failed = 0
   detailed_tests_failed = []
   os.mkdir(directory + "isif3Test")

   # Tests
   
   file = open(directory + "isif3Test" + "/INCAR", "w")
   file.close()
   result = automagician.is_isif3(directory+"isif3Test")
   total_tests += 1
   if result:
      tests_failed+= 1
      detailed_tests_failed.append("INCAR with nothing in it was considered box")
   
   file = open(directory + "isif3Test" + "/INCAR", "w")
   file.write("ISIF = 3")
   file.close() 
   result = automagician.is_isif3(directory+"isif3Test")
   total_tests += 1
   if not result:
      tests_failed+= 1
      detailed_tests_failed.append("INCAR ISIF = 3 not considered box")
   

   file = open(directory + "isif3Test" + "/INCAR", "w")
   file.write("RANDOM JUNK\n")
   file.write("ISIF = 3")
   file.close() 
   result = automagician.is_isif3(directory+"isif3Test")
   total_tests += 1
   if not result:
      tests_failed += 1
      detailed_tests_failed.append("INCAR ISIF = 3 not considered box, with random junk in first line")

   file = open(directory + "isif3Test" + "/INCAR", "w")
   file.write("ISIF = 3 # Comment")
   file.close() 
   result = automagician.is_isif3(directory+"isif3Test")
   total_tests += 1
   if not result:
      tests_failed+= 1
      detailed_tests_failed.append("INCAR ISIF = 3 not considered box, with a comment")

   file = open(directory + "isif3Test" + "/INCAR", "w")
   file.write("#ISIF = 3 ")
   file.close() 
   result = automagician.is_isif3(directory+"isif3Test")
   total_tests += 1
   if result:
      tests_failed+= 1
      detailed_tests_failed.append("INCAR ISIF = 3 commented out still had box")
   
   file = open(directory + "isif3Test" + "/INCAR", "w")
   file.write("# ISIF = 3 ")
   file.close() 
   result = automagician.is_isif3(directory+"isif3Test")
   total_tests += 1
   if result:
      tests_failed+= 1
      detailed_tests_failed.append("INCAR ISIF = 3 commented out with a space in front still had box")
   # Teardown
   os.remove(directory + "isif3Test" + "/INCAR")
   os.rmdir(directory + "isif3Test")
   print_test_results(total_tests, tests_failed, "determine_is_isif3", detailed_tests_failed)
   
def test_determine_box_convergence():
   total_tests = 0
   tests_failed = 0
   detailed_tests_failed = []

   
   # Converged
   value = automagician.determine_box_convergence(test_file_dir + "/box_converged")
   total_tests += 1
   if not value:
      tests_failed+= 1
      detailed_tests_failed.append("Converged box not considered converged")

   # Unconverged
   value = automagician.determine_box_convergence(test_file_dir + "/box_unconverged")
   total_tests += 1
   if value:
      tests_failed+= 1
      detailed_tests_failed.append("Unconverged box not considered converged")
   
   
   print_test_results(total_tests, tests_failed, "determine_box_convergence", detailed_tests_failed)

def test_set_incar_tags(directory:str):
   total_tests = 0
   tests_failed = 0
   detailed_tests_failed = []

   incar = open(directory + "INCAR", "w")
   dict = {}
   incar.close()
   automagician.set_incar_tags(directory + "INCAR", dict)
   incar = open(directory + "INCAR", "r")
   total_tests += 1
   if len(incar.readlines()) != 0:
      tests_failed += 1
      detailed_tests_failed.append("Adding nothing to empty incar failed")
   incar.close()
   os.remove(directory + "INCAR")

   incar = open(directory + "INCAR", "w")
   incar.write("HELLO=5")
   incar.close()
   dict = {}
   dict["HELLO"] = "6"
   automagician.set_incar_tags(directory + "INCAR", dict)
   incar = open(directory + "INCAR", "r")
   total_tests += 1
   if len(incar.readlines()) != 1:
      tests_failed += 1
      detailed_tests_failed.append("Changing line in INCAR incorrect length 1 line expected {} lines found".format(len(incar.readlines())))
   # line = incar.readline()
   for line in incar.readlines() :
      if line != "HELLO=6":
         tests_failed += 1
         detailed_tests_failed.append("Wrong line present {}".format(line))
   incar.close()
   os.remove(directory + "INCAR")

   incar = open(directory + "INCAR", "w")
   incar.write("HELLO=5")
   incar.close()
   dict = {}
   dict["HELLO"] = "5"
   automagician.set_incar_tags(directory + "INCAR", dict)
   incar = open(directory + "INCAR", "r")
   total_tests += 1
   if len(incar.readlines()) != 1:
      tests_failed += 1
      detailed_tests_failed.append("Leaving line in INCAR unchanged incorrect length 1 line expected {} lines found".format(len(incar.readlines())))
   # line = incar.readline()
   for line in incar.readlines() :
      if line != "HELLO=5":
         tests_failed += 1
         detailed_tests_failed.append("Wrong line present {}".format(line))
   incar.close()
   os.remove(directory + "INCAR")


   incar = open(directory + "INCAR", "w")
   incar.write("HELLO=5")
   incar.close()
   dict = {}
   automagician.set_incar_tags(directory + "INCAR", dict)
   incar = open(directory + "INCAR", "r")
   total_tests += 1
   if len(incar.readlines()) != 1:
      tests_failed += 1
      detailed_tests_failed.append("Empty tag dict leaves INCAR unchanged".format(len(incar.readlines())))
   # line = incar.readline()
   for line in incar.readlines() :
      if line != "HELLO=5":
         tests_failed += 1
         detailed_tests_failed.append("Wrong line present {}".format(line))
   incar.close()
   os.remove(directory + "INCAR")

   incar = open(directory + "INCAR", "w")
   incar.close()
   dict = {}
   dict["HELLO"] = "5"
   automagician.set_incar_tags(directory + "INCAR", dict)
   incar = open(directory + "INCAR", "r")
   total_tests += 1
   if len(incar.readlines()) != 1:
      tests_failed += 1
      detailed_tests_failed.append("Adding line leaves unchanged".format(len(incar.readlines())))
   # line = incar.readline()
   for line in incar.readlines() :
      if line != "HELLO=5":
         tests_failed += 1
         detailed_tests_failed.append("Wrong line present {}".format(line))
   incar.close()
   os.remove(directory + "INCAR")

   incar = open(directory + "INCAR", "w")
   incar.write("TEST=3")
   incar.close()
   dict = {}
   dict["HELLO"] = "5"
   dict["TEST"] = "7"
   automagician.set_incar_tags(directory + "INCAR", dict)
   incar = open(directory + "INCAR", "r")
   total_tests += 1
   if len(incar.readlines()) != 2:
      tests_failed += 1
      detailed_tests_failed.append("Adding 2 lines leaves unchanged".format(len(incar.readlines())))
   hello_present = False
   test_present = False
   incar.close()
   incar = open(directory + "INCAR", "r")
   for line in incar.readlines():
      if line != "HELLO=5\n" and line != "TEST=7\n":
         tests_failed += 1
         detailed_tests_failed.append("Wrong line present {}".format(line))
      if hello_present and line == "HELLO=5\n":
         tests_failed += 1
         detailed_tests_failed.append("Line was duplicated")
      if test_present and line == "TEST=7\n":
         tests_failed += 1
         detailed_tests_failed.append("Line was duplicated")
      if line == "HELLO=5\n":
         hello_present = True
      if line == "TEST=7\n":
         test_present = True
   if not hello_present or not test_present:
      tests_failed += 1
      detailed_tests_failed.append("Line that should been present not found")
      # print(hello_present)
      # print(test_present)
   incar.close()
   # os.remove(directory + "INCAR")


   print_test_results(total_tests, tests_failed, "set_incar_tags", detailed_tests_failed)

def main():
   """Runs all of the unit tests for automagician"""
   # make a directory
   directory = os.environ['HOME'] + "/automagician_tests/"
   if os.path.exists(directory):
        shutil.rmtree(directory)
   os.mkdir(directory)
   automagician.parser.values.silent = True



    #run tests
   test_check_has_opt(directory)
   new_test_has_opt()
   test_db_init(directory)
   test_del_pwd(directory)
   test_determine_convergence(directory)
   test_determine_is_isif3(directory)
   test_determine_box_convergence()
   test_set_incar_tags(directory)
    # remove said directory
   shutil.rmtree(directory)
   return None


def print_test_results(total_tests: int, tests_failed: int, method_name: str, detailed_tests_failed):
    if (tests_failed == 0):
        print("All unit tests passed for {}".format(method_name))
    else :
        print("{}/{} tests failed for {}".format(tests_failed, total_tests, method_name))
        for test in detailed_tests_failed:
            print("{}".format(test))


if __name__ == "__main__":
    main()