#!/bin/bash

set -e

main() {
    local dest="/asset/node_modules/@terascope/file-asset-apis"
    if [ -d "$dest" ]; then
        echo "* copying the files from file-asset-apis"
        rm "$dest"
        cp -R ./packages/file-asset-apis/* "$dest"
    fi
}

main "$@"
