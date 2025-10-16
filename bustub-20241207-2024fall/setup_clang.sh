#!/bin/bash

# Ensure we run from the project root where this script lives
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_DIR" || { echo "Failed to cd to script dir: $SCRIPT_DIR"; exit 1; }

# Ensure build directory exists
BUILD_DIR="build"
if [ ! -d "$BUILD_DIR" ]; then
    mkdir -p "$BUILD_DIR"
fi

# Remove old configuration and cached files inside build directory
echo "Cleaning up old build files in '$BUILD_DIR'..."
rm -rf "$BUILD_DIR"/*

echo "Running CMake with Clang-14..."
# Set compiler variables only for the cmake invocation line
CC=clang-14 CXX=clang++-14 cmake -S . -B "$BUILD_DIR" "$@"

if [ $? -ne 0 ]; then
    echo "CMake configuration failed." >&2
    exit 2
fi

echo "Configuration complete. Next step: make -C $BUILD_DIR"
