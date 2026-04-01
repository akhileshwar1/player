#!/bin/sh
set -e

# Build platform executable.
clang \
    -g -O0 \
    -Wall -Wextra \
    -fsanitize=address \
    backtest.cpp \
    -lc\
    -o backtest.out \
