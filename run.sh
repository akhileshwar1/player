#!/bin/bash
while true; do
    env $(cat .env | xargs) ./player.out >> bot.log
    echo "Bot crashed. Restarting..."
    sleep 1
done
