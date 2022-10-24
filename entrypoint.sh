#!/bin/bash
set -e

EXTENSION_DIR=${EXTENSION_DIR:-'/extension'}

C_RESET='\033[0m'
C_I_BLUE='\033[0;94m'
C_I_YELLOW='\033[0;93m'
C_I_PURPLE='\033[0;95m'
C_I_WHITE_UL='\033[1;4;97m'


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
        npm install && npm run build
    fi

    poetry build

    pip install -U pip && pip install -U --force-reinstall $DIST_DIR/*.whl

    popd

    echo "Extension installed."
fi

if [[ "$@" == *bash* ]]; then
    echo

    pyfiglet -f ansi_regular -c BLUE "Connect Extension Runner"
    while read -r line; do echo -e $line; done << EOF

To install your extension execute:

\t${C_I_BLUE}$ poetry install${C_RESET}


To start the Connect Extension Runner execute:

\t${C_I_BLUE}$ cextrun${C_RESET}


You can add the ${C_I_YELLOW}--debug${C_RESET} flag to the ${C_I_BLUE}cextrun${C_RESET} command to set the log level to ${C_I_YELLOW}DEBUG${C_RESET}.

If you want the runner to detect the changes to your python files you can use the ${C_I_YELLOW}--reload${C_RESET} flag to start it in autoreload mode:

\t${C_I_BLUE}$ cextrun${C_RESET} ${C_I_YELLOW}--debug${C_RESET} ${C_I_YELLOW}--reload${C_RESET}

You can also use a pre-configured tmux session that starts the runner in debug and autoreload mode and, ${C_I_WHITE_UL}if your extension provide a UI,${C_RESET}
it also starts webpack in watch mode.

To run it execute:

\t${C_I_BLUE}$ extension-devel${C_RESET}

The following key bindings are configured for you:

\t${C_I_PURPLE}F1${C_RESET} - Switch to runner window
\t${C_I_PURPLE}F2${C_RESET} - Switch to webpack window
\t${C_I_PURPLE}F3${C_RESET} - Send CTRL+C to both windows so the session will be terminated

EOF


fi

exec "$@"
