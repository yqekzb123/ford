#!/bin/bash

# Author: Ming Zhang
# Copyright (c) 2022

BUILD_TARGET=client
BUILD_TYPE=Release
BUILD_BRPC=ON

while getopts "sdb" arg
do
  case $arg in
    s)
      echo "building server";
      BUILD_TARGET="server";
      ;;
    d)
      BUILD_TYPE=Debug;
      ;;
    b)
      BUILD_BRPC=OFF;
      ;;
    ?)
      echo "unkonw argument"
  exit 1
  ;;
  esac
done

if [[ -d build ]]; then
  echo "Build directory exists";
else
  echo "Create build directory";
  mkdir build
fi

CMAKE_CMD="cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} ../ -DBRPC=${BUILD_BRPC}"
echo ${CMAKE_CMD}
cd ./build
${CMAKE_CMD}

if [ "${BUILD_TARGET}" == "server" ];then
  echo "------------------- building server ------------------"
  make index_server lock_table_server data_server page_table_server -j32
else
  echo "------------------- building client + server ------------------"
  make -j32
fi
echo "-------------------- build finish ----------------------"
