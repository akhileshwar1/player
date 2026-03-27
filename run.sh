#!/bin/sh
env $(cat .env | xargs) ./player.out
