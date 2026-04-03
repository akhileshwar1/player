#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>
#include <time.h>

#define MAX_EVENTS 10 
#define MAX_TIME_PERIOD 5 * 60 * 60 * 1000 // 5 hours in milliseconds. 
#define TRADE_FEE 0.1 / 100

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
    uint64 lastTime;
    bool isOpen;
    bool AreEventsApplied;
    bool isSnapshot;
    Posn_type posnType; 
} State;

typedef enum
{
   BUY,
   SELL
} Side;

typedef struct
{
    char *symbol;
    real64 price;
    real64 qty;
} Position;

typedef struct
{
    real64 price;
    real64 qty;
    real64 usdtAfterFee;
    real64 qtyAfterFee;
    Side side;
} Trade;

typedef struct
{
    real64 usdt;
    real64 coin;
} Wallet;

void
LoadTradeEvent(Trade_event *trade, char *line, State *state)
{
    char *token;
    token = strtok(line, ",");
    int i = 0;
    while (token != NULL)
    {
        // printf("field : %s\n", token);
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
            trade->time = (uint64)atof(token);
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
        i++;
    }
}

void
PrintTradeState(State *state)
{
    printf("FORCES===================\n");
    printf("START PRICE %f\n", state->startPrice); 
    printf("SELL PRESSURE %f\n", state->sellPressure); 
    printf("BUY PRESSURE %f\n", state->buyPressure); 
}

void
BufferTradeEvent(Trade_event tradeEvent, Trade_events_buffer *tradeEventsBuffer)
{
    uint16 currentWriteIndex = tradeEventsBuffer->currentWriteIndex;
    uint16 size = tradeEventsBuffer->size;
    if (currentWriteIndex >= size)
    {
        tradeEventsBuffer->currentWriteIndex = currentWriteIndex % size;
    }
    tradeEventsBuffer->buffer[tradeEventsBuffer->currentWriteIndex] = tradeEvent;
    printf("Buffered event at index %u\n",
           tradeEventsBuffer->currentWriteIndex);
    tradeEventsBuffer->currentWriteIndex++;
    tradeEventsBuffer->eventCount++;
}

void
formatMSTimestamp(uint64 ms_timestamp, char *out_buf, size_t buf_sz) {
    time_t seconds = ms_timestamp / 1000;
    int millis = ms_timestamp % 1000;

    struct tm *tm_info = gmtime(&seconds);
    if (!tm_info) {
        snprintf(out_buf, buf_sz, "Invalid Time");
        return;
    }

    char temp[26];
    strftime(temp, sizeof(temp), "%Y-%m-%d %H:%M:%S", tm_info);

    snprintf(out_buf, buf_sz, "%s.%03d", temp, millis);
}

int
main()
{
    FILE *myFile = fopen("data0206.csv", "r");
    if (myFile == NULL)
    {
        printf("Couldn't open file\n");
        return -1;
    }
    FILE *outputFile = fopen("output0206.txt", "w");
    setbuf(outputFile, NULL); // Disables buffering completely
    if (outputFile == NULL)
    {
        printf("Couldn't open file\n");
        return -1;
    }

    fputs("id, Timestamp, symbol, side, price, qty, curr_value, usdt_after_fee, qty_after_fee, pnl\n", outputFile);
    char line[1024];
    State state = {};
    state.TradeEventsBuffer.size = MAX_EVENTS;
    state.TradeEventsBuffer.currentWriteIndex = 0;
    state.buyPressure = 0.0;
    state.sellPressure = 0.0;
    state.timeToRefresh = 30 * 60 * 1000;
    Position position = {};
    position.symbol = "SOLUSDT";
    Wallet wallet = {};
    wallet.usdt = 1000;
    wallet.coin = 100;
    while (fgets(line, sizeof(line), myFile) != NULL)
    {
        char *tmp = strchr(line, '\n');
        if (tmp) *tmp = '\0';

        Trade_event trade = {};
        LoadTradeEvent(&trade, line, &state);
        BufferTradeEvent(trade, &(state.TradeEventsBuffer));
        real64 buyPressure = state.buyPressure;
        real64 sellPressure = state.sellPressure;
        real64 lastPrice = state. 
            TradeEventsBuffer.
            buffer[MAX_EVENTS - 1].price;
        char time_str[32];
        if ( !state.isOpen &&
            lastPrice != 0.0)
        {
            uint64 endTime = trade.time;
            real64 timeElapsedMS = endTime - state.lastTime;

            if (timeElapsedMS > state.timeToRefresh)
            {
                state.startPrice = lastPrice;
                state.lastTime = endTime;
                state.buyPressure = 0.0;
                state.sellPressure = 0.0;
            }

            printf("start price is %f\n", state.startPrice);

            if (abs((lastPrice - state.startPrice)) < 0.5)
            {
                printf("Guilty! There is no price movement\n");
            }
            else
            {
                char body[300];
                if (lastPrice > state.startPrice)
                {
                    if (buyPressure < 2 * sellPressure)
                    {
                        printf("Guilty! Not enough pressure on buy side\n");
                    }
                    else if (timeElapsedMS < 25 * 60 * 1000)
                    {
                        printf("Guilty! Too fast, need real slow and steady!\n");
                    }
                    else
                    {
                        printf("opening the position after %f at lastPrice %f\n",
                               timeElapsedMS,
                               lastPrice);
                        Trade trade = {};
                        real64 qty = 0.1;
                        trade.qty = qty;
                        trade.price = lastPrice;
                        trade.side = BUY;
                        trade.usdtAfterFee = (qty * lastPrice) * (1 - TRADE_FEE);
                        trade.qtyAfterFee = trade.usdtAfterFee / lastPrice; 
                        position.qty += trade.qtyAfterFee;
                        position.price = lastPrice;
                        wallet.coin += trade.qtyAfterFee;
                        wallet.usdt -= qty * lastPrice;
                        formatMSTimestamp(endTime, time_str, sizeof(time_str));
                        // make the order call.
                        sprintf(body, "%lu, %s, %s, %s, %f, %f, %f, %f, %f, %f",
                                endTime,
                                time_str,
                                "SOLUSDT",
                                "BUY",
                                lastPrice,
                                qty,
                                position.qty * position.price,
                                trade.usdtAfterFee,
                                trade.qtyAfterFee,
                                0.0);
                        state.posnType = LONG;
                        // BinanceMakeOrder(curl, body);
                        fputs(strcat(body, "\n"), outputFile);
                        state.timeToClose = timeElapsedMS;
                        state.lastTime = endTime;
                        state.isOpen = true;
                        state.buyPressure = 0.0;
                        state.sellPressure = 0.0;
                    }
                }
                else
                {
                    if (sellPressure < 2 * buyPressure)
                    {
                        printf("Guilty! Not enough pressure on sell side\n");
                    }
                    else if (timeElapsedMS < 25 * 60 * 1000)
                    {
                        printf("Guilty! Too fast, need real slow and steady!\n");
                    }
                    else
                    {
                        printf("opening the position after %f at lastPrice %f\n",
                               timeElapsedMS,
                               lastPrice);
                        Trade trade = {};
                        real64 qty = -0.1;
                        trade.qty = qty;
                        trade.price = lastPrice;
                        trade.side = SELL;
                        trade.qtyAfterFee = (qty) * (1 - TRADE_FEE);
                        trade.usdtAfterFee = trade.qtyAfterFee * lastPrice; 
                        position.qty += trade.qtyAfterFee;
                        position.price = lastPrice;
                        wallet.coin += qty;
                        wallet.usdt -= trade.qtyAfterFee * lastPrice;
                        formatMSTimestamp(endTime, time_str, sizeof(time_str));
                        // make the order call.
                        sprintf(body, "%lu, %s, %s, %s, %f, %f, %f, %f, %f, %f",
                                endTime,
                                time_str,
                                "SOLUSDT",
                                "SELL",
                                lastPrice,
                                qty,
                                position.qty * position.price,
                                trade.usdtAfterFee,
                                trade.qtyAfterFee,
                                0.0);
                        state.posnType = SHORT;
                        // BinanceMakeOrder(curl, body);
                        fputs(strcat(body, "\n"), outputFile);
                        state.timeToClose = timeElapsedMS;
                        state.lastTime = endTime;
                        state.isOpen = true;
                        state.buyPressure = 0.0;
                        state.sellPressure = 0.0;
                    }
                }
            }
        }
        // close the position.
        else if (state.isOpen &&
            lastPrice != 0.0)
        {

            uint64 endTime = trade.time;
            real64 timeElapsedMS = endTime - state.lastTime;
            Posn_type posnType = state.posnType;
            printf("position is open, remaining time %f\n",
                   state.timeToClose - timeElapsedMS);

            if (timeElapsedMS < state.timeToClose)
            {
                printf("Guilty! No need to close, time not out\n");
            }
            else if (posnType == LONG &&
                     (buyPressure > 2 * sellPressure && 
                     ((state.startPrice - lastPrice) > 0.5)) &&
                     (state.timeToClose * 2 < MAX_TIME_PERIOD))
            {
                printf("Guilty!, Loading the position at lastPrice %f\n", lastPrice);
                char body[300];
                Trade trade = {};
                real64 qty = 0.1;
                trade.qty = qty;
                trade.price = lastPrice;
                trade.side = BUY;
                trade.usdtAfterFee = (qty * lastPrice) * (1 - TRADE_FEE);
                trade.qtyAfterFee = trade.usdtAfterFee / lastPrice; 
                position.qty += trade.qtyAfterFee;
                position.price = lastPrice;
                wallet.coin += trade.qtyAfterFee; 
                wallet.usdt -= qty * lastPrice;
                formatMSTimestamp(endTime, time_str, sizeof(time_str));

                // make the order call.
                sprintf(body, "%lu, %s, %s, %s, %f, %f, %f, %f, %f, %f",
                        endTime,
                        time_str,
                        "SOLUSDT",
                        "BUY",
                        lastPrice,
                        qty,
                        position.qty * position.price,
                        trade.usdtAfterFee,
                        trade.qtyAfterFee,
                        0.0);
                // BinanceMakeOrder(curl, body);
                fputs(strcat(body, "\n"), outputFile);
                state.startPrice = lastPrice;
                state.lastTime = endTime;
                state.buyPressure = 0.0;
                state.sellPressure = 0.0;
                state.timeToClose *= 2;

            }
            else if (posnType == SHORT &&
                (sellPressure > 2 * buyPressure && 
                ((state.startPrice - lastPrice) > 0.5)) &&
                (state.timeToClose * 2 < MAX_TIME_PERIOD))
            {
                printf("Guilty!, Loading the position at lastPrice %f\n", lastPrice);
                char body[300];
                Trade trade = {};
                real64 qty = -0.1;
                trade.qty = qty;
                trade.price = lastPrice;
                trade.side = SELL;
                trade.qtyAfterFee = (qty) * (1 - TRADE_FEE);
                trade.usdtAfterFee = trade.qtyAfterFee * lastPrice; 
                position.qty += trade.qtyAfterFee;
                position.price = lastPrice;
                wallet.coin += qty;
                wallet.usdt -= trade.qtyAfterFee * lastPrice; 
                formatMSTimestamp(endTime, time_str, sizeof(time_str));

                // make the order call.
                sprintf(body, "%lu, %s, %s, %s, %f, %f, %f, %f, %f, %f",
                        endTime,
                        time_str,
                        "SOLUSDT",
                        "SELL",
                        lastPrice,
                        qty,
                        position.qty * position.price,
                        trade.usdtAfterFee,
                        trade.qtyAfterFee,
                        0.0); 
                // BinanceMakeOrder(curl, body);
                fputs(strcat(body, "\n"), outputFile);
                state.startPrice = lastPrice;
                state.lastTime = endTime;
                state.buyPressure = 0.0;
                state.sellPressure = 0.0;
                state.timeToClose *= 2;
            }
            else // will only close now when the pressure's have reversed or
                 // time's up.
            {
                printf("closing the position at lastPrice %f\n", lastPrice);
                char body[300];
                if (posnType == LONG)
                {
                    Trade trade = {};
                    real64 currPosValue = position.qty * lastPrice;
                    real64 prevPosValue = position.qty * position.price;
                    real64 qty = -position.qty;
                    trade.qty = qty;
                    trade.price = lastPrice;
                    trade.side = SELL;
                    trade.qtyAfterFee = (qty) * (1 - TRADE_FEE);
                    trade.usdtAfterFee = trade.qtyAfterFee * lastPrice; 
                    position.qty += trade.qtyAfterFee;
                    position.price = lastPrice;
                    wallet.coin += qty;
                    wallet.usdt -= trade.qtyAfterFee * lastPrice; 
                    formatMSTimestamp(endTime, time_str, sizeof(time_str));

                    // make the order call.
                    sprintf(body, "%lu, %s, %s, %s, %f, %f, %f, %f, %f, %f",
                            endTime,
                            time_str,
                            "SOLUSDT",
                            "SELL",
                            lastPrice,
                            qty,
                            position.qty * position.price,
                            trade.usdtAfterFee,
                            trade.qtyAfterFee,
                            currPosValue - prevPosValue); 
                }
                else if (posnType == SHORT)
                {
                    Trade trade = {};
                    real64 currPosValue = position.qty * lastPrice;
                    real64 prevPosValue = position.qty * position.price;
                    real64 qty = -position.qty;
                    trade.qty = qty;
                    trade.price = lastPrice;
                    trade.side = BUY;
                    trade.usdtAfterFee = (qty * lastPrice) * (1 - TRADE_FEE);
                    trade.qtyAfterFee = trade.usdtAfterFee / lastPrice; 
                    position.qty += trade.qtyAfterFee;
                    position.price = lastPrice;
                    wallet.coin += trade.qtyAfterFee; 
                    wallet.usdt -= qty * lastPrice;
                    formatMSTimestamp(endTime, time_str, sizeof(time_str));

                    // make the order call.
                    sprintf(body, "%lu, %s, %s, %s, %f, %f, %f, %f, %f, %f",
                            endTime,
                            time_str,
                            "SOLUSDT",
                            "BUY",
                            lastPrice,
                            qty,
                            position.qty * position.price,
                            trade.usdtAfterFee,
                            trade.qtyAfterFee,
                            currPosValue - prevPosValue); 
                }
                printf("body is %s, api key is %s\n", body, getenv("API_KEY"));
                // BinanceMakeOrder(curl, body);
                fputs(strcat(body, "\n"), outputFile);
                state.posnType = ZERO;
                state.isOpen = false;
                state.startPrice = lastPrice;
                state.lastTime = endTime;
                state.buyPressure = 0.0;
                state.sellPressure = 0.0;
            }
        }
        PrintTradeState(&state);
    }

    fclose(myFile);
}
