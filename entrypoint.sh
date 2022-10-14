#!/bin/bash
set -e

EXTENSION_DIR=${EXTENSION_DIR:-'/extension'}

if [[ "$EXTENSION_DIR"  != /* ]]; then
    echo "Extension directory ($EXTENSION_DIR) must be an absolute path"
    exit 1
fi

if [[ "$@" == *"cextrun"* ]]; then

    if [ -n "${REPOSITORY_URL+set}" ] && [ -n "${COMMIT_ID+set}" ]; then  
        git clone $REPOSITORY_URL $EXTENSION_DIR
        git -C $EXTENSION_DIR checkout $COMMIT_ID
    fi

    if [ ! -d $EXTENSION_DIR ]; then
        echo "Extension directory ($EXTENSION_DIR) not found"
        exit 1
    fi

    if [ ! -f $EXTENSION_DIR/pyproject.toml ]; then
        echo "pyproject.toml not found in $EXTENSION_DIR"
        exit 1
    fi

    DIST_DIR=$EXTENSION_DIR/dist

    echo "Installing the extension package from $EXTENSION_DIR..."
    
    pushd .
    
    cd $EXTENSION_DIR

    if [ -d "$DIST_DIR" ]; then rm -Rf $DIST_DIR; fi

    if [ -f "package.json" ]; then
        npm run build
    fi

    poetry build

    pip install -U pip && pip install -U --force-reinstall $DIST_DIR/*.whl

    popd

    echo "Extension installed."
fi

if [[ "$@" == *bash* ]]; then
    echo "To install the extension execute poetry build inside /extension folder"
    echo "In order to run the extension manually run the command cextrun"
    echo "In order to run the extension in debug mode, please use cextrun -d command"
    echo "In the case that you modified dependencies or want to install development dependencies run the command: poetry install"
fi

exec "$@"
