#!/bin/bash
# Usage: ./scripts/prepare_file.sh <path_to_file>
# Copies file into fileserver/files/ named by its sha256 hash
# and prints the hash + size for use with the download command.

set -e

FILE=$1
if [ -z "$FILE" ]; then
    echo "usage: $0 <file_path>"
    exit 1
fi

mkdir -p fileserver/files
HASH=$(sha256sum "$FILE" | cut -d' ' -f1)
SIZE=$(stat -c%s "$FILE")
cp "$FILE" "fileserver/files/$HASH"

echo "hash: $HASH"
echo "size: $SIZE"
echo ""
echo "file ready at fileserver/files/$HASH"