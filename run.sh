#!/bin/bash
# while true; do
    export ASAN_OPTIONS=detect_leaks=1
    env $(cat .env | xargs) ./player.out >> bot.log 2>&1
    # echo "Bot crashed. Restarting..."
    # sleep 1
# done
