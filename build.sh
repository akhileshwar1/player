#!/bin/sh
set -e

# Build platform executable.
g++ \
    -g -O0 \
    -Wall -Wextra \
    -fsanitize=address \
    -DPLAYER_DEBUG=1 \
    main.cpp \
    -lwebsockets \
    -lyyjson \
    -lcurl \
    -luuid \
    -lcrypto \
    -o player.out \
