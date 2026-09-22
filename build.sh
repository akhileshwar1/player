#!/bin/sh
set -e

g++ \
    -g -O0 \
    -Wall -Wextra \
    -fsanitize=address \
    -DPLAYER_DEBUG=1 \
    main.cpp \
    channel.c \
    -lwebsockets \
    -lyyjson \
    -lcurl \
    -luuid \
    -lcrypto \
    -pthread \
    -o player.out
