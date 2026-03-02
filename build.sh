#!/bin/sh
set -e

# Build platform executable.
clang \
    -g -O0 \
    -Wall -Wextra \
    -fsanitize=address \
    -DPLAYER_DEBUG=1 \
    main.cpp \
    -lwebsockets \
    -lyyjson \
    -o player.out \
