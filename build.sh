#!/bin/sh
set -e

# Build platform executable.
clang \
    -g -O0 \
    -Wall -Wextra \
    -fsanitize=address \
    main.cpp \
    -lwebsockets \
    -lyyjson \
    -o player.out \
