#ifndef CHANNEL_H
#define CHANNEL_H

#include <stdbool.h>
#include <pthread.h>

#define CHANNEL_LENGTH 10

typedef struct
{
    bool  isEmpty;
    void *data;
} Slot;

typedef struct
{
    Slot buffer[CHANNEL_LENGTH];
    int  putIndex;
    int  takeIndex;

    pthread_mutex_t mutex;
} Channel;

typedef struct
{                               /* Used as argument to thread_start() */
    pthread_t thread_id;        /* ID returned by pthread_create() */
    int       thread_num;       /* Application-defined thread # */
    Channel *channel;
} thread_info;

void ChannelInit(Channel *channel);

bool ChannelPut(Channel *channel, void *data);

void *ChannelTake(Channel *channel);

void ChannelDestroy(Channel *channel);

#endif
