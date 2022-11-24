#!/bin/bash
set -e

EXTENSION_DIR=${EXTENSION_DIR:-'/extension'}
RUNNING_MODE=local


if [[ "$EXTENSION_DIR"  != /* ]]; then
    echo "Extension directory ($EXTENSION_DIR) must be an absolute path"
    exit 1
fi


if [ -n "${REPOSITORY_URL+set}" ] && [ -n "${COMMIT_ID+set}" ]; then  
    git clone $REPOSITORY_URL $EXTENSION_DIR
    git -C $EXTENSION_DIR checkout $COMMIT_ID
    RUNNING_MODE=cloud
fi


if [ ! -d $EXTENSION_DIR ]; then
    echo "Extension directory ($EXTENSION_DIR) not found"
    exit 1
fi

cd $EXTENSION_DIR

if [[ "$1" == "cextrun" ]]; then

    if [ ! -f $EXTENSION_DIR/pyproject.toml ]; then
        echo "pyproject.toml not found in $EXTENSION_DIR"
        exit 1
    fi

    poetry install
    if [[ "$RUNNING_MODE" == "local" ]] && [[ -f $EXTENSION_DIR/package.json ]]; then
        test ! -L "node_modules" && ln -s /install_temp/node_modules .
        npm run build --if-present
    fi

fi


if [[ "$1" == "extension-test" ]] || [[ "$1" == "extension-devel" ]]; then
    poetry install

    if [[ "$RUNNING_MODE" == "local" ]] && [[ -f $EXTENSION_DIR/package.json ]]; then
        test ! -L "node_modules" && ln -s /install_temp/node_modules .
        npm run build --if-present
    fi
fi

if [[ "$1" == *bash* ]] && [[ "$#" -eq 1 ]] && [[ -f pyproject.toml ]]; then

    poetry install

    if [[ "$RUNNING_MODE" == "local" ]] && [[ -f $EXTENSION_DIR/package.json ]]; then
        test ! -L "node_modules" && ln -s /install_temp/node_modules .
        npm run build --if-present
    fi

    echo

    pyfiglet -f ansi_regular -c BLUE "Connect Extension Runner"

    if [[ "$RUNNING_MODE" == "local" ]] && [[ -f $EXTENSION_DIR/package.json ]]; then
        while read -r line; do echo -e $line; done < /banners/banner_ui
    else
        while read -r line; do echo -e $line; done < /banners/banner
    fi

fi

exec "$@"
