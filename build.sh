#!/bin/bash
set -e

clean(){
    echo "removing .venv"
    rm -rf .venv
    rm -rf .mypy_cache
    rm -rf .pytest_cache
    rm -rf dist
    rm -rf src/automagician/__pycache__
    rm -rf src/automagician/automagician/egg-info
    rm -rf test/__pycache__
    rm .coverage
}

create() {
    echo "creating venv"
    uv venv .venv
    echo "leaving create"
}

lint() {
    echo "linting code"
    ruff format src test
    isort src test --profile=black
    mypy src --disallow-untyped-defs --strict
    ruff check src test
}

install_dev() {
    echo "installing dev dependencies"
    uv pip install -e ".[dev,remote]"
}

test(){
    echo "running tests"
    pytest --cov-report term-missing --cov=automagician test/
}

build(){
    uv build
}


case $1 in 
    clean)
        clean
        ;;
    activate)
        echo "Activating is broken use \"source .venv/bin/activate\" to activate the venv"
        ;;
    create)
        create
        ;;
    lint)
        lint
        ;;
    install_dev)
        install_dev
        ;;
    test)
        test
        ;;
    build)
        build
        ;;
    release)
        lint
        test
        build
        ;;
    help)
        echo "The commands in here are:"
        echo "clean -- remove the virtutual enviorment"
        echo "activate -- print how to acitvate a virtual enviorment"
        echo "create -- create a virtual enviorment"
        echo "lint -- run formatters and static analyxers"
        echo "install_dev -- install dependencies required for developentn"
        echo "test -- run unit tests"
        echo "build -- create the whl file"
        echo "release -- Use for to run lint, test, and build scripts."
        ;;
    *)
        echo "unknown command -- use build.sh help to get the valid commands"
        ;;
    esac
