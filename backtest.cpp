#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#define MAX_EVENTS 10 

typedef uint32_t uint32;
typedef uint64_t uint64;
typedef float real32;
typedef double real64;
typedef uint16_t uint16;

typedef struct
{
    char *e;
    char *s;
    uint64 id;
    real64 price;
    real64 quantity;
    uint64 time;
    bool bmaker; // is buyer the market maker?
} Trade_event;

typedef struct
{
    uint16 currentWriteIndex;
    uint16 size;
    uint16 eventCount;
    Trade_event buffer[MAX_EVENTS];
} Trade_events_buffer;

typedef enum
{
    LONG,
    SHORT,
    ZERO
} Posn_type;

typedef struct timespec timespec;
typedef struct
{
    char *event;
    Trade_events_buffer TradeEventsBuffer;
    real64 startPrice;
    real64 timeToClose;
    real64 timeToRefresh;
    real64 buyPressure;
    real64 sellPressure;
    timespec lastTime;
    bool isOpen;
    bool AreEventsApplied;
    bool isSnapshot;
    Posn_type posnType; 
} State;

void
LoadTradeEvent(Trade_event *trade, char *line, State *state)
{
    char *token;
    token = strtok(line, ",");
    int i = 0;
    while (token != NULL)
    {
        printf("field : %s\n", token);
        if (i == 0)
        {
            trade->id = (uint64)atoi(token);
        }
        else if (i ==  1)
        {
            trade->price = (real64)atof(token);
        }
        else if (i ==  2)
        {
            trade->quantity = (real64)atof(token);
        }
        else if (i ==  4)
        {
            trade->time = (uint64)atoi(token);
        }
        else if (i ==  5)
        {
            if (strcasecmp(token, "true") == 0)
            {
                trade->bmaker = true;
            }
            else
            {
                trade->bmaker = false;
            }
        }

        if (trade->bmaker)
        {
            state->sellPressure += trade->quantity;
            printf("Sell pressure added to %f\n", state->sellPressure);
        }
        else
        {
            state->buyPressure += trade->quantity;
            printf("Buy pressure added to %f\n", state->buyPressure);
        }
        token = strtok(NULL, ",");
    }
}

int
main()
{
    FILE *myFile = fopen("data.csv", "r");
    if (myFile == NULL)
    {
        printf("Couldn't open file\n");
        return -1;
    }

    char line[1024];
    State state = {};
    while (fgets(line, sizeof(line), myFile) != NULL)
    {
        char *tmp = strchr(line, '\n');
        if (tmp) *tmp = '\0';

        Trade_event trade = {};
        LoadTradeEvent(&trade, line, &state);
    }

    fclose(myFile);
}
