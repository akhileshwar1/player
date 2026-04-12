#!/bin/bash
# while true; do
    env $(cat .env | xargs) ./player.out >> bot.log 2>&1
    # echo "Bot crashed. Restarting..."
    # sleep 1
# done
