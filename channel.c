#include "channel.h"
#include <stdio.h>

void
ChannelInit(Channel *channel)
{
    channel->putIndex = 0;
    channel->takeIndex = 0;

    for (int i = 0; i < CHANNEL_LENGTH; i++)
    {
        channel->buffer[i].isEmpty = true;
        channel->buffer[i].data = NULL;
    }

    pthread_mutex_init(&channel->mutex, NULL);
}

bool
ChannelPut(Channel *channel, void *data)
{
    pthread_mutex_lock(&channel->mutex);

    /*
     * Ring buffer is full if advancing putIndex
     * would make it equal to takeIndex.
     */
    int nextIndex =
        (channel->putIndex + 1) % CHANNEL_LENGTH;

    if (nextIndex == channel->takeIndex)
    {
        printf("Buffer full, can't put!\n");

        pthread_mutex_unlock(&channel->mutex);
        return false;
    }

    channel->buffer[channel->putIndex].data = data;
    channel->buffer[channel->putIndex].isEmpty = false;

    printf("put data at %p\n",
           channel->buffer[channel->putIndex].data);

    channel->putIndex = nextIndex;

    pthread_mutex_unlock(&channel->mutex);

    return true;
}

void *
ChannelTake(Channel *channel)
{
    pthread_mutex_lock(&channel->mutex);

    /*
     * Empty if putIndex == takeIndex.
     */
    if (channel->putIndex == channel->takeIndex)
    {
        pthread_mutex_unlock(&channel->mutex);
        return NULL;
    }

    /*
     * IMPORTANT:
     * Read the current slot BEFORE advancing takeIndex.
     */
    void *data =
        channel->buffer[channel->takeIndex].data;

    printf("take data at %p\n", data);

    channel->buffer[channel->takeIndex].data = NULL;
    channel->buffer[channel->takeIndex].isEmpty = true;

    channel->takeIndex =
        (channel->takeIndex + 1) % CHANNEL_LENGTH;

    pthread_mutex_unlock(&channel->mutex);

    return data;
}

void
ChannelDestroy(Channel *channel)
{
    pthread_mutex_destroy(&channel->mutex);
}
