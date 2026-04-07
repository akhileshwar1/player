#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <libwebsockets.h>
#include <yyjson.h>
#include <curl/curl.h>
#include <openssl/hmac.h>
#include <openssl/evp.h>

#define ArrayCount(Array) (sizeof(Array) / sizeof(Array[0]))
#define Assert(Expression) if(!(Expression)) {*(int *)0 = 0;}
#define MARKET_BASE_ENDP "stream.binance.com"
#define STREAM_PATH "/ws/solusdt@depth"
#define TRADE_STREAM_PATH "/ws/solusdt@trade"
#define MAX_LEVELS 10
#define MAX_EVENTS 10 
#define MAX_TIME_PERIOD 5 * 60 * 60 * 1000 // 5 hours in milliseconds. 
#define TRADE_FEE 0.1 / 100
#define SNAPSHOT_URL "https://api.binance.com/api/v3/depth?symbol=SOLUSDT&limit=10"
#define TRADE_URL "https://api.binance.com/api/v3/order?"

typedef uint32_t uint32;
typedef uint64_t uint64;
typedef float real32;
typedef double real64;
typedef uint16_t uint16;

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

typedef struct 
{
    real64 quantity;
    real64 price;
} quote;

typedef struct
{
    char *e;
    uint64 E;
    char *s;
    uint64 U;
    uint64 u;
    quote asks[MAX_LEVELS]; // 10 levels on each side.
    quote bids[MAX_LEVELS];
} Market_event;

typedef struct
{
    uint16 currentWriteIndex;
    uint16 size;
    uint16 eventCount;
    Market_event buffer[MAX_EVENTS];
} Market_events_buffer;

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

typedef struct {
    char *resp;
    size_t size;
} Snapshot;

typedef struct
{
    uint64 lastUpdateId;
    quote asks[MAX_LEVELS]; // store from best ask/lowest ask to the worst ask.
    quote bids[MAX_LEVELS]; // store from best bid/highest bid to the worst bid.
} Order_book;

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
    Market_events_buffer MarketEventsBuffer;
    Trade_events_buffer TradeEventsBuffer;
    Snapshot Snapshot;
    Order_book OrderBook;
    real64 startPrice;
    real64 timeToClose;
    real64 timeToRefresh;
    real64 buyPressure;
    real64 sellPressure;
    timespec lastTime;
    bool isOpen;
    bool AreEventsApplied;
    bool isSnapshot;
    bool isPriceTaken;
    CURL *curl;
    Posn_type posnType; 
    Position position;
    Wallet wallet;
    FILE *outputFile;
} State;

void formatMSTimestamp(uint64_t ms_timestamp, char *out_buf, size_t buf_sz) {
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

real64 XtimeElapsedMS (timespec lastTime, timespec endTime) {
    real64 timeElapsedSeconds = endTime.tv_sec - lastTime.tv_sec;
    real64 timeElapsedNanoSeconds = endTime.tv_nsec - lastTime.tv_nsec;
    real64 timeElapsedMS = (timeElapsedSeconds * 1000.0f)  + (timeElapsedNanoSeconds / (1000.0f * 1000.0f));
    return timeElapsedMS;
}

uint32
StringLength(char *str)
{
    uint32 length = 0;
    while(*str++)
    {
        length++;
    }
    return length;
}

char 
*StringCat(char *str1, char *str2)
{
    uint32 length = StringLength(str1) + StringLength(str2) + 1;
    printf("length is %u\n", length);
    char *buffer = (char *)malloc(length*sizeof(char));
    if (buffer == NULL)
    {
        free(buffer);
        return NULL;
    }

    char *ptr = buffer;
    while (*str1 != '\0')
    {
        *ptr++ = *str1++;
    }

    while(*str2 != '\0')
    {
        *ptr++ = *str2++;
    }
    *ptr = '\0';
    return buffer;
}

void
AddLevelsToEvent(yyjson_val *val, quote *quotes)
{
    size_t idx;
    size_t max;
    yyjson_val *array;
    yyjson_arr_foreach(val, idx, max, array)
    {
        if (idx >= MAX_LEVELS) break; // only read the max levels.
        yyjson_val *a;
        quote q = {};
        size_t j, jmax;
        yyjson_arr_foreach(array, j, jmax, a)
        {
            char *endptr;
            const char *str = yyjson_get_str(a);
            float f = strtod(str, &endptr);
            if (str == endptr)
            {
                printf("Failed str to float conversion\n");
            }
            if (j == 0)
            {
                q.price = f;
            }
            else
            {
                q.quantity = f;
            }
            // printf("q %d, %f\n", (int)j, f);
        }

        quotes[idx] = q;
    }
}

yyjson_doc
*IsEventComplete(char *input)
{
    yyjson_doc *doc = yyjson_read(input, StringLength(input), 0);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *e = yyjson_obj_get(root, "e");
    if (e == NULL) return NULL;
    else return doc;
}

void
LoadMarketEvent(yyjson_doc *doc, Market_event *event)
{
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *e = yyjson_obj_get(root, "e");
    event->e = (char *)yyjson_get_str(e);
    yyjson_val *E = yyjson_obj_get(root, "E");
    event->E = (uint64)yyjson_get_int(E);
    yyjson_val *s = yyjson_obj_get(root, "s");
    event->s = (char *)yyjson_get_str(s);
    yyjson_val *u = yyjson_obj_get(root, "u");
    event->u = (uint64)yyjson_get_int(u);
    yyjson_val *U = yyjson_obj_get(root, "U");
    event->U = (uint64)yyjson_get_int(U);
    yyjson_val *b = yyjson_obj_get(root, "b");
    yyjson_val *a = yyjson_obj_get(root, "a");
    printf("event type is %s\n", yyjson_get_str(e));
    AddLevelsToEvent(a, event->asks);
    AddLevelsToEvent(b, event->bids);
    yyjson_doc_free(doc);
}

void
BufferEvent(Market_event marketEvent, Market_events_buffer *marketEventsBuffer)
{
    uint16 currentWriteIndex = marketEventsBuffer->currentWriteIndex;
    uint16 size = marketEventsBuffer->size;
    if (currentWriteIndex >= size)
    {
        marketEventsBuffer->currentWriteIndex = currentWriteIndex % size;
    }
    marketEventsBuffer->buffer[marketEventsBuffer->currentWriteIndex] = marketEvent;
    printf("Buffered event U %lu at index %u\n",
           marketEvent.U,
           marketEventsBuffer->currentWriteIndex);
    marketEventsBuffer->currentWriteIndex++;
    marketEventsBuffer->eventCount++;
}

// TODO(Akhil): abstract the above and below into one func.
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
ApplyEvent(Market_event event, Order_book *OrderBook)
{
    quote *eventAsks = event.asks;
    quote *eventBids = event.bids;
    quote *OBAsks = OrderBook->asks;
    quote *OBBids = OrderBook->bids;

    for (int i = 0; i < MAX_LEVELS; i++)
    {
        quote eventAsk = eventAsks[i];
        if (eventAsk.price == 0.0) continue;
        else // update or insert or remove.
        {
            int insertAt = -1;
            int removeAt = -1;
            for (int j = 0; j < MAX_LEVELS; j++)
            {
                if (eventAsk.quantity == 0.0 &&
                    eventAsk.price == OBAsks[j].price)
                {
                    removeAt = j;
                    printf("removing ask at posn %d\n", removeAt);
                    break;
                }
                else if (eventAsk.price == OBAsks[j].price)
                {
                    OBAsks[j].quantity = eventAsk.quantity;
                    break;
                }
                else if ((eventAsk.quantity != 0.0 &&
                          eventAsk.price < OBAsks[j].price) || // insert the holes formed by removal.
                         (eventAsk.quantity != 0.0 &&
                          OBAsks[j].price == 0.0))
                {
                    insertAt = j;
                    printf("inserting ask at posn %d\n", insertAt);
                    break;
                }
            }

            quote OBAsksCopy[MAX_LEVELS];
            if (insertAt != -1)
            {
                // copy first to shift down later.
                for (int i = 0; i < MAX_LEVELS; i++)
                {
                    OBAsksCopy[i] = OBAsks[i];
                }

                // insert at the position.
                OBAsks[insertAt] = eventAsk;

                // shift down now.
                for (int i = insertAt + 1; i < MAX_LEVELS; i++)
                {
                    OBAsks[i] = OBAsksCopy[i - 1];
                }
            }
            else if (removeAt != -1)
            {
                // copy first to shift up later.
                for (int i = 0; i < MAX_LEVELS; i++)
                {
                    OBAsksCopy[i] = OBAsks[i];
                }

                // shift up now and insert an hole at the end.
                for (int i = removeAt; i < MAX_LEVELS; i++)
                {
                    if (i == MAX_LEVELS - 1)
                    {
                        printf("inserting hole at the end\n");
                        OBAsks[i].price = 0.0;
                    }
                    else
                    {
                        OBAsks[i] = OBAsksCopy[i + 1];
                    }
                }
            }
        }
    }

    // go for the bids now.
    for (int i = 0; i < MAX_LEVELS; i++)
    {
        quote eventBid = eventBids[i];
        if (eventBid.price == 0.0) continue;
        else // update or insert or remove.
        {
            int insertAt = -1;
            int removeAt = -1;
            for (int j = 0; j < MAX_LEVELS; j++)
            {
                if (eventBid.quantity == 0.0 &&
                    eventBid.price == OBBids[j].price)
                {
                    removeAt = j;
                    printf("removing bid at posn %d\n", removeAt);
                    break;
                }
                else if (eventBid.price == OBBids[j].price)
                {
                    OBBids[j].quantity = eventBid.quantity;
                    break;
                }
                else if((eventBid.quantity != 0.0 &&
                         eventBid.price > OBBids[j].price) || // insert the holes formed by removal.
                         (eventBid.quantity != 0.0 &&
                          OBBids[j].price == 0.0)) 
                {
                    insertAt = j;
                    printf("inserting bid at posn %d\n", insertAt);
                    break;
                }
            }

            quote OBBidsCopy[MAX_LEVELS];
            if (insertAt != -1)
            {
                // copy first to shift down later.
                for (int i = 0; i < MAX_LEVELS; i++)
                {
                    OBBidsCopy[i] = OBBids[i];
                }

                // insert at the position.
                OBBids[insertAt] = eventBid;

                // shift down now.
                for (int i = insertAt + 1; i < MAX_LEVELS; i++)
                {
                    OBBids[i] = OBBidsCopy[i - 1];
                }
            }
            else if (removeAt != -1)
            {
                // copy first to shift up later.
                for (int i = 0; i < MAX_LEVELS; i++)
                {
                    OBBidsCopy[i] = OBBids[i];
                }

                // shift up now and insert an hole at the end.
                for (int i = removeAt; i < MAX_LEVELS; i++)
                {
                    if (i == MAX_LEVELS - 1)
                    {
                        printf("inserting hole at the end\n");
                        OBBids[i].price = 0.0;
                    }
                    else
                    {
                        OBBids[i] = OBBidsCopy[i + 1];
                    }
                }
            }
        }
    }
}

void
LoadBufferAndApplyEvent(Market_event marketEvent, State *state, yyjson_doc *doc)
{
    Order_book *OrderBook = &state->OrderBook;
    Market_events_buffer *MarketEventsBuffer = &state->MarketEventsBuffer;
    bool AreEventsApplied = state->AreEventsApplied;
    LoadMarketEvent(doc, &marketEvent);
    if (!AreEventsApplied)
    {
        BufferEvent(marketEvent, MarketEventsBuffer);
    }
    else
    {
        ApplyEvent(marketEvent, OrderBook);
    }
}

uint64
BinanceTimestamp() {
    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    // Convert to milliseconds
    return ((uint64)(ts.tv_sec) * 1000) + ((uint64)(ts.tv_nsec) / 1000000);
}

void generate_signature(const char* query, const char* secret, char* out_hex) {
    unsigned char hash[32];
    unsigned int len = 32;

    HMAC(EVP_sha256(), secret, strlen(secret), 
         (unsigned char*)query, strlen(query), hash, &len);

    for (int i = 0; i < 32; i++) {
        sprintf(out_hex + (i * 2), "%02x", hash[i]);
    }
}

bool
BinanceMakeOrder(CURL *curl, char *body)
{
    curl_easy_reset(curl);
    char signed_body[2048];
    char signature[65]; // 64 hex chars + null terminator

    struct curl_slist *headers = NULL;
    char key_header[128];
    snprintf(key_header, sizeof(key_header), "X-MBX-APIKEY: %s", getenv("API_KEY"));
    headers = curl_slist_append(headers, key_header);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, 0L);
    // Generate the hex signature from the current body
    char *secret = getenv("API_SECRET");
    generate_signature(body, getenv("API_SECRET"), signature);

    // Combine the body and the signature
    snprintf(signed_body, sizeof(signed_body), "%s&signature=%s", body, signature);
    printf("signed body is %s\n", signed_body);
    curl_easy_setopt(curl, CURLOPT_URL, StringCat(TRADE_URL,
                                                  signed_body));
    CURLcode result = curl_easy_perform(curl);
    long http_code = 0;
    if (result == CURLE_OK) {
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

        if (http_code == 200) {
            printf("Success! Order placed.\n");
            return true;
        } else {
            printf("API Error! HTTP Status: %ld\n", http_code);
            return false;
        }
    }
    else
    {
        printf("Transfer failed: %s\n", curl_easy_strerror(result));
        return false;
    }
}

int
CallbackBinance(struct lws *wsi,
                enum lws_callback_reasons reason,
                void *user, void *in, size_t len)
{
    switch (reason)
    {
        case LWS_CALLBACK_CLIENT_ESTABLISHED:
            printf("callback_binance: LWS_CALLBACK_CLIENT_ESTABLISHED\n");
            break;

        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
            printf("LWS_CALLBACK_CLIENT_CONNECTION_ERROR\n");
            break;

        case LWS_CALLBACK_CLOSED:
            printf("LWS_CALLBACK_CLOSED\n");
            break;

        case LWS_CALLBACK_CLIENT_RECEIVE:
            {
                ((char *)in)[len] = '\0';
                printf("rx %d '%s'\n", (int)len, (char *)in);
                Market_event marketEvent = {};
                // TODO(Akhil): There's a double copy happening here,
                //              could be simpler.
                yyjson_doc *doc = IsEventComplete((char *)in);
                if (doc == NULL)
                {
                    ((State *)user)->event = StringCat(((State *)user)->event, (char *)in);
                }
                else
                {
                    LoadBufferAndApplyEvent(marketEvent, (State *)user, doc);
                }

                doc = IsEventComplete(((State *)user)->event); 
                if (doc != NULL)
                {
                    printf("NOT null anymore %s\n", ((State *)user)->event);
                    LoadBufferAndApplyEvent(marketEvent, (State *)user, doc);
                    ((State *)user)->event = "";
                }
                break;
            }

        default:
            break;
    }
    return 0;
}

void
LoadTradeEvent(Trade_event *trade, char *input, State *state)
{
    yyjson_doc *doc = yyjson_read(input , StringLength(input), 0);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *e = yyjson_obj_get(root, "e");
    trade->e = (char *)yyjson_get_str(e);
    yyjson_val *s = yyjson_obj_get(root, "s");
    trade->s = (char *)yyjson_get_str(s);
    yyjson_val *id = yyjson_obj_get(root, "t");
    trade->id = (uint64)yyjson_get_int(s);
    yyjson_val *price = yyjson_obj_get(root, "p");
    trade->price = (real64)atof(yyjson_get_str(price));
    yyjson_val *quantity = yyjson_obj_get(root, "q");
    trade->quantity = (real64)atof(yyjson_get_str(quantity));
    yyjson_val *time = yyjson_obj_get(root, "T");
    trade->time = (real64)(yyjson_get_int(time));
    yyjson_val *bmaker = yyjson_obj_get(root, "m");
    trade->bmaker = yyjson_get_bool(bmaker);
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
}

int
CallbackBinanceTrade(struct lws *wsi,
                enum lws_callback_reasons reason,
                void *user, void *in, size_t len)
{
    switch (reason)
    {
        case LWS_CALLBACK_CLIENT_ESTABLISHED:
            printf("callback_binance: LWS_CALLBACK_CLIENT_ESTABLISHED\n");
            break;

        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
            printf("LWS_CALLBACK_CLIENT_CONNECTION_ERROR\n");
            break;

        case LWS_CALLBACK_CLOSED:
            printf("LWS_CALLBACK_CLOSED\n");
            break;

        case LWS_CALLBACK_CLIENT_RECEIVE:
            {
                ((char *)in)[len] = '\0';
                printf("rx Trade %d '%s'\n", (int)len, (char *)in);
                char time_str[32];
                Position position = ((State *)user)->position;
                Wallet wallet = ((State *)user)->wallet;
                FILE *outputFile = ((State *)user)->outputFile;
                State state = *((State *)user);
                Trade_event tradeEvent = {};
                LoadTradeEvent(&tradeEvent, (char *)in, (State *)user);
                BufferTradeEvent(tradeEvent, &((State *)user)->TradeEventsBuffer);
                real64 buyPressure = ((State *)user)->buyPressure;
                real64 sellPressure = ((State *)user)->sellPressure;
                real64 lastPrice = ((State *)user)->
                    TradeEventsBuffer.
                    buffer[MAX_EVENTS - 1].price;

                printf("last price is %f\n", lastPrice);
                if (!(((State *)user)->isPriceTaken))
                {
                    ((State *)user)->startPrice = lastPrice;
                    ((State *)user)->isPriceTaken = true;
                }

                if (!((State *)user)->isOpen &&
                    lastPrice != 0.0)
                {
                    timespec endTime;
                    clock_gettime(CLOCK_MONOTONIC_RAW, &endTime);
                    real64 timeElapsedMS = XtimeElapsedMS(
                        ((State *)user)->lastTime,
                        endTime
                    );

                    if (timeElapsedMS > ((State *)user)->timeToRefresh)
                    {
                        ((State *)user)->startPrice = lastPrice;
                        ((State *)user)->lastTime = endTime;
                        ((State *)user)->buyPressure = 0.0;
                        ((State *)user)->sellPressure = 0.0;
                    }

                    printf("start price is %f\n", ((State *)user)->startPrice);

                    if (abs((lastPrice - ((State *)user)->startPrice)) < 0.5)
                    {
                        printf("Guilty! There is no price movement\n");
                    }
                    else
                    {
                        CURL *curl = ((State *)user)->curl;
                        uint64 timestamp = BinanceTimestamp();
                        char body[300];
                        if (lastPrice > (((State *)user)->startPrice))
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
                                // make the order call.
                                sprintf(body, "symbol=%s&side=%s&type=%s&quantity=%f&timestamp=%lu",
                                        "SOLUSDT",
                                        "BUY",
                                        "MARKET",
                                        0.1,
                                        timestamp);
                                printf("body is %s, api key is %s\n", body, getenv("API_KEY"));
                                bool res = BinanceMakeOrder(curl, body);
                                if (res)
                                {
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
                                    formatMSTimestamp(timestamp, time_str, sizeof(time_str));
                                    sprintf(body, "%lu, %s, %s, %s, %f, %f, %f, %f, %f, %f",
                                            timestamp,
                                            time_str,
                                            "SOLUSDT",
                                            "BUY",
                                            lastPrice,
                                            qty,
                                            position.qty * position.price,
                                            trade.usdtAfterFee,
                                            trade.qtyAfterFee,
                                            0.0);
                                    fputs(strcat(body, "\n"), outputFile);
                                    ((State *)user)->posnType = LONG;
                                    ((State *)user)->timeToClose = timeElapsedMS;
                                    ((State *)user)->lastTime = endTime;
                                    ((State *)user)->isOpen = true;
                                    ((State *)user)->buyPressure = 0.0;
                                    ((State *)user)->sellPressure = 0.0;
                                }
                                
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
                                // make the order call.
                                sprintf(body, "symbol=%s&side=%s&type=%s&quantity=%f&timestamp=%lu",
                                        "SOLUSDT",
                                        "SELL",
                                        "MARKET",
                                        0.1,
                                        timestamp);
                                printf("body is %s, api key is %s\n", body, getenv("API_KEY"));
                                bool res = BinanceMakeOrder(curl, body);
                                if (res)
                                {
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
                                    formatMSTimestamp(timestamp, time_str, sizeof(time_str));
                                    // make the order call.
                                    sprintf(body, "%lu, %s, %s, %s, %f, %f, %f, %f, %f, %f",
                                            timestamp,
                                            time_str,
                                            "SOLUSDT",
                                            "SELL",
                                            lastPrice,
                                            qty,
                                            position.qty * position.price,
                                            trade.usdtAfterFee,
                                            trade.qtyAfterFee,
                                            0.0);
                                    fputs(strcat(body, "\n"), outputFile);
                                    ((State *)user)->posnType = SHORT;
                                    ((State *)user)->timeToClose = timeElapsedMS;
                                    ((State *)user)->lastTime = endTime;
                                    ((State *)user)->isOpen = true;
                                    ((State *)user)->buyPressure = 0.0;
                                    ((State *)user)->sellPressure = 0.0;

                                }
                            }
                        }
                    }
                }
                // close the position.
                else if (((State *)user)->isOpen &&
                         lastPrice != 0.0)
                {

                    timespec endTime;
                    clock_gettime(CLOCK_MONOTONIC_RAW, &endTime);
                    real64 timeElapsedMS = XtimeElapsedMS(
                        ((State *)user)->lastTime,
                        endTime
                    );
                    Posn_type posnType = ((State *)user)->posnType;
                    printf("position is open, remaining time %f\n",
                           ((State *)user)->timeToClose - timeElapsedMS);

                    if(timeElapsedMS < state.timeToClose &&
                        ((posnType == LONG && ((lastPrice - state.startPrice) * position.qty) > -0.05) ||
                        (posnType == SHORT && ((lastPrice - state.startPrice) * position.qty) > -0.05))) 
                    {
                        printf("Guilty! No need to close, time not out and loss in check\n");
                    }
                    else if(posnType == LONG &&
                            (buyPressure > 2 * sellPressure && 
                            ((state.startPrice - lastPrice) > 0.5)) &&
                            (state.timeToClose * 2 < MAX_TIME_PERIOD)) 
                    {
                        printf("Guilty! No need to close, long pressure not reversed, so Load up!\n");
                        printf("Loading the position at lastPrice %f\n", lastPrice);
                        CURL *curl = ((State *)user)->curl;
                        char body[300];
                        uint64 timestamp = BinanceTimestamp();
                        sprintf(body, "symbol=%s&side=%s&type=%s&quantity=%f&timestamp=%lu",
                                "SOLUSDT",
                                "SELL",
                                "MARKET",
                                0.1,
                                timestamp);
                        printf("body is %s, api key is %s\n", body, getenv("API_KEY"));
                        if (BinanceMakeOrder(curl, body))
                        {
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
                            formatMSTimestamp(timestamp, time_str, sizeof(time_str));

                            sprintf(body, "%lu, %s, %s, %s, %f, %f, %f, %f, %f, %f",
                                    timestamp,
                                    time_str,
                                    "SOLUSDT",
                                    "BUY",
                                    lastPrice,
                                    qty,
                                    position.qty * position.price,
                                    trade.usdtAfterFee,
                                    trade.qtyAfterFee,
                                    0.0);
                            fputs(strcat(body, "\n"), outputFile);
                            ((State *)user)->startPrice = lastPrice;
                            ((State *)user)->lastTime = endTime;
                            ((State *)user)->buyPressure = 0.0;
                            ((State *)user)->sellPressure = 0.0;
                            ((State *)user)->timeToClose *= 2;
                        }
                    }
                    else if(posnType == SHORT &&
                            (sellPressure > 2 * buyPressure && 
                            ((state.startPrice - lastPrice) > 0.5)) &&
                            (state.timeToClose * 2 < MAX_TIME_PERIOD)) 
                    {
                        printf("Guilty! No need to close, short pressure not reversed, so Load up!\n");
                        printf("Loading the position at lastPrice %f\n", lastPrice);
                        CURL *curl = ((State *)user)->curl;
                        char body[300];
                        uint64 timestamp = BinanceTimestamp();
                        sprintf(body, "symbol=%s&side=%s&type=%s&quantity=%f&timestamp=%lu",
                                "SOLUSDT",
                                "SELL",
                                "MARKET",
                                0.1,
                                timestamp);
                        printf("body is %s, api key is %s\n", body, getenv("API_KEY"));
                        if (BinanceMakeOrder(curl, body))
                        {
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
                            formatMSTimestamp(timestamp, time_str, sizeof(time_str));

                            // make the order call.
                            sprintf(body, "%lu, %s, %s, %s, %f, %f, %f, %f, %f, %f",
                                    timestamp,
                                    time_str,
                                    "SOLUSDT",
                                    "SELL",
                                    lastPrice,
                                    qty,
                                    position.qty * position.price,
                                    trade.usdtAfterFee,
                                    trade.qtyAfterFee,
                                    0.0); 
                            fputs(strcat(body, "\n"), outputFile);
                            ((State *)user)->startPrice = lastPrice;
                            ((State *)user)->lastTime = endTime;
                            ((State *)user)->buyPressure = 0.0;
                            ((State *)user)->sellPressure = 0.0;
                            ((State *)user)->timeToClose *= 2;
                        }
                    }
                    else // will only close now when the pressure's have reversed.
                    {
                        printf("closing the position at lastPrice %f\n", lastPrice);
                        CURL *curl = ((State *)user)->curl;
                        char body[300];
                        uint64 timestamp = BinanceTimestamp();
                        if (posnType == LONG)
                        {
                            sprintf(body, "symbol=%s&side=%s&type=%s&quantity=%f&timestamp=%lu",
                                    "SOLUSDT",
                                    "SELL",
                                    "MARKET",
                                    0.1,
                                    timestamp);
                            if (BinanceMakeOrder(curl, body))
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
                                formatMSTimestamp(timestamp, time_str, sizeof(time_str));

                                sprintf(body, "%lu, %s, %s, %s, %f, %f, %f, %f, %f, %f",
                                        timestamp,
                                        time_str,
                                        "SOLUSDT",
                                        "SELL",
                                        lastPrice,
                                        qty,
                                        position.qty * position.price,
                                        trade.usdtAfterFee,
                                        trade.qtyAfterFee,
                                        currPosValue - prevPosValue);
                                fputs(strcat(body, "\n"), outputFile);
                                ((State *)user)->posnType = ZERO;
                                ((State *)user)->isOpen = false;
                                ((State *)user)->startPrice = lastPrice;
                                ((State *)user)->lastTime = endTime;
                                ((State *)user)->buyPressure = 0.0;
                                ((State *)user)->sellPressure = 0.0;
                            }
                        }
                        else if (posnType == SHORT)
                        {
                            sprintf(body, "symbol=%s&side=%s&type=%s&quantity=%f&timestamp=%lu",
                                    "SOLUSDT",
                                    "BUY",
                                    "MARKET",
                                    0.1,
                                    timestamp);
                            if (BinanceMakeOrder(curl, body))
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
                                formatMSTimestamp(timestamp, time_str, sizeof(time_str));

                                sprintf(body, "%lu, %s, %s, %s, %f, %f, %f, %f, %f, %f",
                                        timestamp,
                                        time_str,
                                        "SOLUSDT",
                                        "BUY",
                                        lastPrice,
                                        qty,
                                        position.qty * position.price,
                                        trade.usdtAfterFee,
                                        trade.qtyAfterFee,
                                        currPosValue - prevPosValue);
                                fputs(strcat(body, "\n"), outputFile);
                                ((State *)user)->posnType = ZERO;
                                ((State *)user)->isOpen = false;
                                ((State *)user)->startPrice = lastPrice;
                                ((State *)user)->lastTime = endTime;
                                ((State *)user)->buyPressure = 0.0;
                                ((State *)user)->sellPressure = 0.0;
                            }
                        }
                        printf("body is %s, api key is %s\n", body, getenv("API_KEY"));
                    }
                }
            }

        default:
            break;
    }
    return 0;
}

static struct lws_protocols protocols[] = {
    {
        "binance",
        CallbackBinance,
        0,
        1024,
    },
    {
        "binance-trade",
        CallbackBinanceTrade,
        0,
        1024,
    },
    { NULL, NULL, 0, 0 }    // Terminator - ALWAYS REQUIRED
};

/* transfer the newly arrived contents in buffer to already
 * existing struct on userp
*/
size_t
write_data(void *buffer, size_t size, size_t nmemb, void *userp)
{
    size_t realsize = size * nmemb;
    Snapshot *snapshot = (Snapshot *)userp;
    snapshot->size = 0;
    char *ptr = (char *)realloc(snapshot->resp, realsize  + 1);
    if (!ptr) return 0;

    snapshot->resp = ptr;
    memcpy(snapshot->resp, buffer, realsize);
    snapshot->size = realsize;
    snapshot->resp[snapshot->size] = 0;

    printf("in write call back, size is %zu, resp is %s\n", realsize, snapshot->resp);
    return realsize;
}

// NOTE(Akhil): why void? can't the load fail? json wrong?
void
SetOrderBook(State *state)
{
    Order_book *OrderBook = &state->OrderBook;
    Snapshot *Snapshot = &state->Snapshot;
    char *input = (char *)Snapshot->resp;
    yyjson_doc *doc = yyjson_read(input, StringLength(input), 0);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *id = yyjson_obj_get(root, "lastUpdateId");
    uint64 lastUpdateId = (uint64)yyjson_get_int(id);
    printf("Last update id is %lu\n", lastUpdateId);
    OrderBook->lastUpdateId = lastUpdateId;

    yyjson_val *bids = yyjson_obj_get(root, "bids");
    yyjson_val *asks = yyjson_obj_get(root, "asks");
    AddLevelsToEvent(asks, OrderBook->asks);
    AddLevelsToEvent(bids, OrderBook->bids);
}

void
PrintOrderBook(State *state)
{
    Order_book *OrderBook = &state->OrderBook;
    printf("BIDS===================\n");
    quote *bids = OrderBook->bids;
    quote *asks = OrderBook->asks;
    for (int i = 0; i < MAX_LEVELS; i++)
    {
        printf("Price: %f, quantity: %f\n", bids[i].price, bids[i].quantity);
    }

    printf("ASKS===================\n");
    for (int i = 0; i < MAX_LEVELS; i++)
    {
        printf("Price: %f, quantity: %f\n", asks[i].price, asks[i].quantity);
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
IgnoreAndApplyEvents(State *state)
{
    Order_book *OrderBook = &state->OrderBook;
    Market_events_buffer *MarketEventsBuffer = &state->MarketEventsBuffer;
    bool *isSnapshot = &state->isSnapshot;
    bool *AreEventsApplied = &state->AreEventsApplied;
    uint64 lastUpdateId = OrderBook->lastUpdateId;
    bool applied = false;
    for (int i = 0; i < MAX_EVENTS; i++)
    {
        Market_event event = MarketEventsBuffer->buffer[i];
        uint64 firstId = event.U; 
        uint64 lastId = event.u; 
        printf("Compare: lastId %lu , firstId %lu, and lastUpdateId %lu\n",
               lastId, firstId, lastUpdateId);
        if (lastId <= lastUpdateId)
        {
            printf("Continuing\n");
            continue; // Ignore.
        }
        else if ((firstId - lastUpdateId) == 1)
        {
            printf("Applying the event\n");
            ApplyEvent(event, OrderBook);
            lastUpdateId = lastId;
            applied = true;
            state->startPrice = (OrderBook->asks)[0].price;
            clock_gettime(CLOCK_MONOTONIC_RAW, &(state->lastTime));
        }
        else
        {
            // Missed some events, rework the entire snapshot.
            printf("Rework the snapshot\n");
        }
    }

    if (applied)
    {
       *AreEventsApplied = true;
    }
    else
    {
        *isSnapshot = false;
        *AreEventsApplied = false;
    }
}

int
main()
{
    CURLcode res = curl_global_init(CURL_GLOBAL_ALL);
    if (res != CURLE_OK) {
        printf("curl setup failed, abort!");
        return -1;
    }

    CURL *curl = curl_easy_init();
    if (curl == NULL) {
        printf("curl setup failed, abort!");
        return -1;
    }

    FILE *outputFile = fopen("outputLive.csv", "w");
    setbuf(outputFile, NULL); // Disables buffering completely
    if (outputFile == NULL)
    {
        printf("Couldn't open file\n");
        return -1;
    }

    fputs("id, Timestamp, symbol, side, price, qty, curr_value, usdt_after_fee, qty_after_fee, pnl\n", outputFile);
    State state = {};
    state.curl = curl;
    state.event = "";
    state.isSnapshot = false;
    state.isPriceTaken = false;
    // state.MarketEventsBuffer.size = MAX_EVENTS;
    // state.MarketEventsBuffer.currentWriteIndex = 0;
    state.TradeEventsBuffer.size = MAX_EVENTS;
    state.TradeEventsBuffer.currentWriteIndex = 0;
    state.buyPressure = 0.0;
    state.sellPressure = 0.0;
    state.timeToRefresh = 2 * 60 * 1000;
    Position position = {};
    position.symbol = "SOLUSDT";
    Wallet wallet = {};
    wallet.usdt = 71.62;
    wallet.coin = 16.02;
    state.wallet = wallet;
    state.position = position;
    state.outputFile = outputFile;
    lws_set_log_level(LLL_ERR | LLL_WARN | LLL_NOTICE | LLL_USER, NULL);
    printf("running\n");
    char *address = StringCat(MARKET_BASE_ENDP, STREAM_PATH);
    printf("address is %s\n", address);
    free(address);
    struct lws_context_creation_info info = {};
    info.port = CONTEXT_PORT_NO_LISTEN;
    info.options = LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT; // Crucial for SSL
    info.protocols = protocols;
    info.gid = -1;
    info.uid = -1;
    int port = 9443;

    struct lws_context *context = lws_create_context(&info);
    if (context == NULL)
    {
        printf("Couldn't create context\n");
        return -1;
    }
    struct lws_protocols protocol = {};
    // protocol.name = "binance";
    // protocol.callback = CallbackBinance;
    // protocol.per_session_data_size = 256;
    //
    // struct lws_client_connect_info ccinfo = {};
    // ccinfo.context = context;
    // ccinfo.address = MARKET_BASE_ENDP;
    // ccinfo.port = port;
    // ccinfo.ssl_connection = 1;
    // ccinfo.path = STREAM_PATH;
    // ccinfo.host = ccinfo.address;
    // ccinfo.origin = ccinfo.address;
    // ccinfo.ssl_connection = LCCSCF_USE_SSL;
    // ccinfo.ietf_version_or_minus_one = -1;
    // ccinfo.protocol = "binance";
    // ccinfo.userdata = (void *)&state;
    // struct lws *lws = lws_client_connect_via_info(&ccinfo);
    // if (lws == NULL)
    // {
    //     printf("Connection failed\n");
    //     return -1;
    // }

    struct lws_protocols protocolTrade = {};
    protocol.name = "binance-trade";
    protocol.callback = CallbackBinanceTrade;
    protocol.per_session_data_size = 256;

    struct lws_client_connect_info ccinfoTrade = {};
    ccinfoTrade.context = context;
    ccinfoTrade.address = MARKET_BASE_ENDP;
    ccinfoTrade.port = port;
    ccinfoTrade.ssl_connection = 1;
    ccinfoTrade.path = TRADE_STREAM_PATH;
    ccinfoTrade.host = ccinfoTrade.address;
    ccinfoTrade.origin = ccinfoTrade.address;
    ccinfoTrade.ssl_connection = LCCSCF_USE_SSL;
    ccinfoTrade.ietf_version_or_minus_one = -1;
    ccinfoTrade.protocol = "binance-trade";
    ccinfoTrade.userdata = (void *)&state;
    struct lws *lwsTrade = lws_client_connect_via_info(&ccinfoTrade);
    if (lwsTrade == NULL)
    {
        printf("Connection failed\n");
        return -1;
    }

    while(1)
    {
        // if (state.MarketEventsBuffer.currentWriteIndex > 0 &&
        //     !state.isSnapshot) {
        //     printf("Checking for snapshot...\n");
        //     curl_easy_setopt(curl, CURLOPT_URL, SNAPSHOT_URL);
        //     curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
        //     curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&state.Snapshot);
        //     CURLcode result = curl_easy_perform(curl);
        //     if (result != CURLE_OK) {
        //         printf("curl call failed!, %s abort!\n", curl_easy_strerror(result));
        //     }
        //     char *input = (char *)state.Snapshot.resp;
        //     yyjson_doc *doc = yyjson_read(input, StringLength(input), 0);
        //     yyjson_val *root = yyjson_doc_get_root(doc);
        //     yyjson_val *id = yyjson_obj_get(root, "lastUpdateId");
        //     uint64 lastUpdateId = (uint64)yyjson_get_int(id);
        //     printf("Last update id is %lu\n", lastUpdateId);
        //     Market_event firstEvent = state.MarketEventsBuffer.buffer[0];
        //     printf("first update id is %lu\n", firstEvent.U);
        //     printf("Compare: lastUpdateId %lu with first event id %lu\n",
        //            lastUpdateId, firstEvent.U);
        //     printf("current Write inDex is %u\n",
        //            state.MarketEventsBuffer.currentWriteIndex);
        //     if (lastUpdateId > firstEvent.U) {
        //         printf("LastUpdateId %lu > the first buffered event id!", lastUpdateId);
        //         state.isSnapshot = true;
        //     }
        // }
        // else if (state.isSnapshot &&
        //          !state.AreEventsApplied)
        // {
        //     SetOrderBook(&state);
        //     printf("Order book id is %lu\n", state.OrderBook.lastUpdateId);
        //     // discard/ignore the buffered events where the id < snapshot id
        //     // apply the buffered events to the order book
        //     IgnoreAndApplyEvents(&state);
        // }

        // apply the event to the order book in the callback, if the OB is ready.
        lws_service(context, 0);
        // PrintOrderBook(&state);
        PrintTradeState(&state);
    }


    lws_context_destroy(context);
    return 0;
}
