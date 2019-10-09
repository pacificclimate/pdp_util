#!/bin/bash
# Run a single download. Results go in zipfile/directory $1. URL to hit is $2.
# Any NetCDF files downloaded are dumped (ncdump) directly to the destination directory.
# Caution: Don't download huge files, because dumping them will make even huger files.
dest=$1
url=$2
curl $url --output "$dest.zip"
unzip "$dest.zip" -d "$dest"
for f in $(find "$dest" -name "*.nc")
do
  ncdump $f >"$dest/$(basename $f).txt"
done