#!/bin/sh
set -e

    # -fsanitize=address \
g++ \
    -g -O0 \
    -Wall -Wextra \
    -DPLAYER_DEBUG=1 \
    main.cpp \
    channel.c \
    log.c \
    -lwebsockets \
    -lyyjson \
    -lcurl \
    -luuid \
    -lcrypto \
    -pthread \
    -o player.out
