# Using the build framework
The main build script is in `build.sh`. 

## Setting up for development
1. Clone the code to your directory using `git clone  /home/jw53959/public_automagician_repo .`

2. Change directory to that with the pulled code 

3. To set up, one should run `build.sh create` to create 
a virtual environment. Then use `build.sh activate` to 
get instructions on how to activate it 

    * If you are using VS Code with the python 
    extension the virtual environment may already be 
    activated

4. To install the dependencies needed for development
run `build.sh install_dev` to install dependencies into
your virtual environment, and installs automagician in 
editable mode, meaning that you can test it without
having to build automagician

## Development
*  `build.sh release` should be the main command that 
you run during development. It lints, formats, runs tests
and builds the code. 
* `build.sh test` simply runs tests on automagician
* `build.sh lint` formats your code, then typechecks it,
then runs flake8 on it
* `build.sh build` builds your package. There are
little uses for running this command as 
`build.sh install_dev` already installs your package in 
editable mode, meaning that changes in code can be
tested without having to re-build and re-install your
code
# Testing

The testing framework used in this repository is pytest.
This makes it much easier to write tests as things like 
creating of temporary test files is much easier for us. 

The main idea used in the tests in this repository is as
follows.
* Set up the required parameters
* Call the function under test
* Make assertions about what should have happened

Note: the usage of Function under test. 
Currently the scope of unit testing is at the function 
level. This includes both functions that do not call
other functions outside the standard library, and 
functions that do a lot of work. To make the job of 
testing the high level functions easier it is acceptable
to assume sane inputs to test the logic in the function
under test, and not the functions that it calls, Which 
can be assumed to be tested.

## On Mocking
Try to avoid mocking out dependencies. Currently the 
only mock in this testing code is to mock out the use 
of sbatch so that running unit tests does not 
submit a job. This means that if you need a database 
to test a function, use pytests `tmp_path` feature and 
create a database for this specific test case

## On Test Files
Test files belong in `test_files`. Ensure that tests do 
not write to these files. It is generally better to 
copy whatever files you need into the `tmp_path`
that your test runs so your test can avoid overwriting
test_files used by other tests

## On Coverage
Even though pytest-coverage is set up by `build.sh test`
100% coverage is not necessarily the best. This is 
because a passing test says simply that no exceptions 
happened during the test execution. So ensure that your 
tests have good "assertion coverage" as well

# Constants
Constants go in `constants.py` The idea for separating 
all constants into a single space is that it makes it
easier to connect constants with their users, and make 
it possible to safely change these constants. It could 
be possible to have multiple places to store constants
but currently the code base is small enough that
one singular constants file is good enough. 

Note: Many constants are not in `constants.py` at the
 time of writing