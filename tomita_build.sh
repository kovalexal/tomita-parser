#!/bin/sh

mkdir build
cd build
cmake ../src/ -DCMAKE_BUILD_TYPE=Release
make
cd ..
