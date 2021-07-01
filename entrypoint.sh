#!/bin/bash
set -e

EXTENSION_DIR=${EXTENSION_DIR:-'/extension'}

DIST_DIR=$EXTENSION_DIR/dist

echo "Installing the extension package from $EXTENSION_DIR..."

pushd .

cd $EXTENSION_DIR

if [ -d "$DIST_DIR" ]; then rm -Rf $DIST_DIR; fi

poetry build

pip install -U pip && pip install -U --force-reinstall $DIST_DIR/*.whl

popd

echo "Extension installed."

exec "$@"