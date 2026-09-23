#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <libwebsockets.h>
#include <yyjson.h>
#include <curl/curl.h>
#include <openssl/hmac.h>
#include <openssl/evp.h>
#include <malloc.h>
#include <uuid/uuid.h>
#include <time.h>
#include <math.h>
#include "channel.h"
#include "log.h"

#define ArrayCount(Array) (sizeof(Array) / sizeof(Array[0]))
#define Assert(Expression) if(!(Expression)) {*(int *)0 = 0;}
#define MARKET_BASE_ENDP "stream.binance.com"
#define TRADE_BASE_ENDP "ws-api.binance.com"
#define TRADE_PATH "/ws-api/v3"
#define STREAM_PATH "/ws/solusdt@depth"
#define TRADE_STREAM_PATH "/ws/solusdt@trade"
#define MAX_LEVELS 30
#define MAX_EVENTS 100 
#define MAX_TIME_PERIOD 5 * 60 * 60 * 1000 // 5 hours in milliseconds. 
#define TRADE_FEE 0.1 / 100
#define SNAPSHOT_URL "https://api.binance.com/api/v3/depth?symbol=SOLUSDT&limit=30"
#define TRADE_URL "https://api.binance.com/api/v3/order?"
#define MIN_REFRESH_TIME 5000 
#define MAX_ORDERS 3 
#define SPREAD_LEVEL 20 /* nth level on asks and bids is the spread. */ 

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
    real64 ltp;
    real64 qty;
    real64 pnl;
    uint64 timestamp;
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

typedef enum
{
    OPENBUY,
    OPENSELL,
    LOADBUY,
    LOADSELL,
    CLOSELONG,
    CLOSESHORT,
    MARKET,
    LIMIT
} Order_type;

typedef enum
{
    PENDING,
    COMPLETED,
    CANCELLED
} Order_status;

typedef struct 
{
    char id[100];
    char *coin;
    real64 price;
    real64 qty;
    Order_type type;
    Order_status status;
    Side side;
    uint64 timestamp;
} Order;

typedef struct timespec timespec;
typedef struct
{
    char event[4096 * 10];
    Market_events_buffer MarketEventsBuffer;
    Trade_events_buffer TradeEventsBuffer;
    Snapshot snapshot;
    Order_book OrderBook;
    real64 startPrice;
    real64 startPriceParent;
    real64 timeToClose;
    real64 timeToRefresh;
    real64 timeToRefreshParent;
    real64 buyPressure;
    real64 sellPressure;
    real64 buyPressureParent;
    real64 sellPressureParent;
    real64 SL; /* stop loss in absolute value */
    timespec lastTime;
    timespec lastTimeDashboard;
    timespec lastTimeParent;
    bool isOpen;
    bool AreEventsApplied;
    bool isSnapshot;
    bool isPriceTaken;
    Posn_type posnType; 
    Position position;
    Wallet wallet;
    FILE *outputFile;
    time_t lastTradeTime;
    bool shouldPlaceOrder;
    Order_type orderType;
    Order orders[MAX_ORDERS];
    int currOrderIndex;
} State;

// Define a structure to hold your per-session (per-connection) data
struct per_session_data__minimal {
    unsigned char *buffer; // Pointer to the data to be sent
    size_t len;           // Total length of data remaining
    size_t ptr;           // Current read position in the buffer
    State *state;
    Channel *channel;
};

typedef struct
{
    Channel *channel;

    struct lws_context *context;
    struct lws *wsi;

    struct per_session_data__minimal *pss;
} TradeThreadArgs;

static const char *
SideString(Side side)
{
    switch (side)
    {
        case BUY:  return "BUY";
        case SELL: return "SELL";
        default:   return "?";
    }
}

// static const char *
// OrderTypeString(Order_type type)
// {
//     switch (type)
//     {
//         case OPENBUY:   return "OPENBUY";
//         case OPENSELL:  return "OPENSELL";
//         case LOADBUY:   return "LOADBUY";
//         case LOADSELL:  return "LOADSELL";
//         case CLOSELONG: return "CLOSELONG";
//         case CLOSESHORT:return "CLOSESHORT";
//         case MARKET:    return "MARKET";
//         case LIMIT:     return "LIMIT";
//         default:        return "?";
//     }
// }

static const char *
OrderStatusString(Order_status status)
{
    switch (status)
    {
        case PENDING:   return "PENDING";
        case COMPLETED: return "COMPLETED";
        case CANCELLED: return "CANCELLED";
        default:        return "?";
    }
}

static void
PrintTimestamp(uint64 timestamp)
{
    time_t seconds = timestamp / 1000;

    struct tm tm_time;

    localtime_r(&seconds, &tm_time);

    char buf[32];

    strftime(
        buf,
        sizeof(buf),
        "%H:%M:%S",
        &tm_time
    );

    LogInfo("%s", buf);
}

const char* OrderSideString[] =
    {
        "BUY",
        "SELL"
    };

const char* OrderTypeString[] =
    {
        "OPENBUY",
        "OPENSELL",
        "LOADBUY",
        "LOADSELL",
        "CLOSELONG",
        "CLOSESHORT",
        "MARKET",
        "LIMIT"
    };

void formatMSTimestamp(uint64_t ms_timestamp, char *out_buf, size_t buf_sz) {
    time_t seconds = ms_timestamp / 1000;
    
    int millis = ms_timestamp % 1000;

    struct tm *tm_info = gmtime(&seconds);
    if (!tm_info) {
        LogInfo(out_buf, buf_sz, "Invalid Time");
        return;
    }

    char temp[26];
    strftime(temp, sizeof(temp), "%Y-%m-%d %H:%M:%S", tm_info);

    LogInfo(out_buf, buf_sz, "%s.%03d", temp, millis);
}

long long XgetTimestamp()
{
    timespec ts;
    
    // CLOCK_REALTIME measures wall-clock time since the Epoch
    clock_gettime(CLOCK_REALTIME, &ts); 
    
    long long milliseconds = ((long long)ts.tv_sec * 1000) + (ts.tv_nsec / 1000000);
    return milliseconds;
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
*StringCpy(char *buffer, char *str2)
{
    if (buffer == NULL) return NULL;

    char *ptr = buffer;
    while (*str2 != '\0')
    {
        *ptr++ = *str2++;
    }
    *ptr = '\0';
    return buffer;
}

char 
*StringCat(char *buffer, char *str2)
{
    if (buffer == NULL) return NULL;

    // iterate until the terminating byte to overwrite it below.
    char *ptr = buffer;
    while (*ptr != '\0')
    {
        ptr++;
    }

    while(*str2 != '\0')
    {
        *ptr++ = *str2++;
    }
    *ptr = '\0';
    return buffer;
}

static const char *
BinanceOrderStatusString(Order_status status)
{
    switch (status)
    {
        case PENDING:   return "PENDING";
        case COMPLETED: return "COMPLETED";
        case CANCELLED: return "CANCELLED";
        default:        return "UNKNOWN";
    }
}

void
generateUUID(char *uuidStr)
{
    uuid_t binuuid;
    // Length: 36 characters + 1 null terminator
    char uuid_str[37]; 

    // Generate random UUID (Version 4)
    uuid_generate_random(binuuid);

    // Convert the binary UUID into its standard string representation
    uuid_unparse(binuuid, uuidStr);

    LogInfo("Generated UUID: %s\n", uuidStr);
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

static void
UpdateOrderFromUserData(
    State *state,
    yyjson_val *event
)
{
    yyjson_val *clientOrderId =
        yyjson_obj_get(event, "c");

    yyjson_val *orderStatus =
        yyjson_obj_get(event, "X");

    yyjson_val *executionType =
        yyjson_obj_get(event, "x");

    if (!clientOrderId || !orderStatus)
    {
        LogError("executionReport missing c or X");
        return;
    }

    const char *localOrderId =
        yyjson_get_str(clientOrderId);

    const char *binanceStatus =
        yyjson_get_str(orderStatus);

    const char *binanceExecution =
        executionType
            ? yyjson_get_str(executionType)
            : NULL;

    if (!localOrderId || !binanceStatus)
    {
        LogError("Invalid executionReport");
        return;
    }

    for (int i = 0; i <= state->currOrderIndex; i++)
    {
        Order *order = &state->orders[i];

        if (strcmp(order->id, localOrderId) != 0)
            continue;

        /*
         * Binance order status -> local status
         */
        if (strcmp(binanceStatus, "NEW") == 0 ||
            strcmp(binanceStatus, "PARTIALLY_FILLED") == 0)
        {
            order->status = PENDING;
        }
        else if (strcmp(binanceStatus, "FILLED") == 0)
        {
            LogInfo("order filled\n");
            order->status = COMPLETED;

            /*
             * Use actual execution information from the
             * user-data event, not the original order qty/price.
             */
            yyjson_val *executedQtyVal =
                yyjson_obj_get(event, "l");

            yyjson_val *executionPriceVal =
                yyjson_obj_get(event, "L");

            real64 executedQty =
                executedQtyVal
                    ? atof(yyjson_get_str(executedQtyVal))
                    : 0.0;

            real64 executionPrice =
                executionPriceVal
                    ? atof(yyjson_get_str(executionPriceVal))
                    : 0.0;

            LogInfo(
                "FILLED %s qty=%.8f price=%.8f",
                order->id,
                executedQty,
                executionPrice
            );

            /*
             * Position update.
             */
            Assert(
                state->position.symbol == NULL ||
                strcmp(
                    state->position.symbol,
                    order->coin
                ) == 0
            )

            real64 signedQty =
                (order->side == SELL)
                    ? -executedQty
                    : executedQty;

            real64 oldQty =
                state->position.qty;

            real64 oldPrice =
                state->position.price;

            real64 newQty =
                oldQty + signedQty;

            /*
             * Opening / adding to a position.
             */
            if (oldQty == 0.0)
            {
                state->position.price = executionPrice;
            }
            else if (
                (oldQty > 0.0 && signedQty > 0.0) ||
                (oldQty < 0.0 && signedQty < 0.0)
            )
            {
                /*
                 * Adding to the same side:
                 * weighted average entry price.
                 */
                state->position.price =
                    (
                        fabs(oldQty) * oldPrice +
                        fabs(signedQty) * executionPrice
                    )
                    /
                    fabs(newQty);
            }
            else if (newQty == 0.0)
            {
                /*
                 * Fully closed.
                 */
                state->position.price = 0.0;
            }

            state->position.qty =
                newQty;

            state->position.ltp =
                executionPrice;

            /* realised pnl */
            state->position.pnl = (signedQty * executionPrice) + (oldQty * oldPrice);
            state->position.timestamp =
                order->timestamp;
        }
        else if (
            strcmp(binanceStatus, "CANCELED") == 0 ||
            strcmp(binanceStatus, "EXPIRED") == 0 ||
            strcmp(binanceStatus, "REJECTED") == 0
        )
        {
            order->status = CANCELLED;
        }
        else
        {
            LogWarn(
                "Unknown Binance order status '%s' for %s",
                binanceStatus,
                localOrderId
            );

            return;
        }

        LogInfo(
            "Order %s -> %s (execution=%s)",
            order->id,
            binanceStatus,
            binanceExecution ? binanceExecution : ""
        );

        return;
    }

    LogWarn(
        "Received executionReport for unknown order %s",
        localOrderId
    );
}

static void
UpdateOrderFromBinance(
    State *state,
    yyjson_val *result
)
{
    yyjson_val *clientOrderId =
        yyjson_obj_get(result, "clientOrderId");

    yyjson_val *origClientOrderId =
        yyjson_obj_get(result, "origClientOrderId");

    yyjson_val *status =
        yyjson_obj_get(result, "status");

    if (!status)
    {
        LogError("Binance order response missing status");
        return;
    }

    const char *binanceStatus =
        yyjson_get_str(status);

    if (!binanceStatus)
    {
        LogError("Invalid Binance order status");
        return;
    }

    /*
     * For a cancel response, origClientOrderId refers
     * to the clientOrderId of the order being cancelled.
     *
     * For a normal order.place response, clientOrderId
     * identifies the newly placed order.
     */
    const char *localOrderId = NULL;

    if (strcmp(binanceStatus, "CANCELED") == 0 &&
        origClientOrderId)
    {
        localOrderId = yyjson_get_str(origClientOrderId);
    }
    else if (clientOrderId)
    {
        localOrderId = yyjson_get_str(clientOrderId);
    }

    if (!localOrderId)
    {
        LogError(
            "Couldn't determine local order ID from Binance response"
        );
        return;
    }

    /*
     * Find our local order.
     */
    for (int i = 0; i <= state->currOrderIndex; i++)
    {
        Order *order = &state->orders[i];

        if (strcmp(order->id, localOrderId) != 0)
            continue;

        /*
         * Translate Binance status into our local status.
         */
        if (strcmp(binanceStatus, "NEW") == 0 ||
            strcmp(binanceStatus, "PARTIALLY_FILLED") == 0)
        {
            order->status = PENDING;
        }
        else if (strcmp(binanceStatus, "FILLED") == 0)
        {
            printf("order filled\n");
            order->status = COMPLETED;
            /* assumes that there orders are for one position */
            Assert(0 == strcmp(state->position.symbol, order->coin))
            real64 qty = (order->side == SELL) ? -order->qty : order->qty;
            state->position.qty += qty;
            state->position.price = ((state->position.qty * state->position.price) +
                                    (qty * order->price)) / qty;
            state->position.ltp = order->price;
            state->position.timestamp = order->timestamp;
            state->position.pnl = qty * order->price +
                state->position.price *
                state->position.qty;
        }
        else if (strcmp(binanceStatus, "CANCELED") == 0 ||
                 strcmp(binanceStatus, "EXPIRED") == 0 ||
                 strcmp(binanceStatus, "REJECTED") == 0)
        {
            order->status = CANCELLED;
        }
        else
        {
            LogWarn(
                "Unknown Binance order status '%s' for %s",
                binanceStatus,
                localOrderId
            );
            return;
        }

        LogInfo(
            "Order %s -> %s",
            order->id,
            binanceStatus
        );

        return;
    }

    LogWarn(
        "Received Binance response for unknown order %s",
        localOrderId
    );
}

static int
CallbackBinanceTrade(struct lws *wsi, enum lws_callback_reasons reason,
                     void *user, void *in, size_t len)
{
    // Cast the user storage pointer to our session structure
    struct per_session_data__minimal *pss = 
        (struct per_session_data__minimal *)user;

    switch (reason) {
        case LWS_CALLBACK_CLIENT_ESTABLISHED:
            {
                printf("TRADE WS ESTABLISHED\n");

                char uuidStr[37];
                generateUUID(uuidStr);

                uint64 timestamp = BinanceTimestamp();

                /*
     * Parameters to sign.
     * Only these parameters are present in the request,
     * so this is the canonical payload.
     */
                char body[512];

                snprintf(
                    body,
                    sizeof(body),
                    "apiKey=%s&timestamp=%lu",
                    getenv("API_KEY_SUB"),
                    timestamp
                );

                char signature[65];

                generate_signature(
                    body,
                    getenv("API_SECRET_SUB"),
                    signature
                );

                yyjson_mut_doc *doc =
                    yyjson_mut_doc_new(NULL);

                yyjson_mut_val *root =
                    yyjson_mut_obj(doc);

                yyjson_mut_doc_set_root(doc, root);

                yyjson_mut_obj_add_str(
                    doc,
                    root,
                    "id",
                    uuidStr
                );

                yyjson_mut_obj_add_str(
                    doc,
                    root,
                    "method",
                    "userDataStream.subscribe.signature"
                );

                yyjson_mut_val *params =
                    yyjson_mut_obj(doc);

                yyjson_mut_obj_add_str(
                    doc,
                    params,
                    "apiKey",
                    getenv("API_KEY_SUB")
                );

                yyjson_mut_obj_add_str(
                    doc,
                    params,
                    "signature",
                    signature
                );

                yyjson_mut_obj_add_int(
                    doc,
                    params,
                    "timestamp",
                    timestamp
                );

                yyjson_mut_obj_add_val(
                    doc,
                    root,
                    "params",
                    params
                );

                char *json =
                    yyjson_mut_write(doc, 0, NULL);

                LogInfo("User data subscribe: %s", json);

                ChannelPut(pss->channel, json);

                // free(json);
                yyjson_mut_doc_free(doc);

                break;
            }

        case LWS_CALLBACK_CLIENT_WRITEABLE:
            {
                LogInfo("WRITEABLE=========\n");
                // 1. Check if we actually have data left to send
                if (!pss || !pss->buffer || pss->ptr >= pss->len) {
                    break;
                }

                // 2. Determine how much data to send in this single frame
                size_t remaining = pss->len - pss->ptr;
                size_t chunk_size = (remaining > 1024) ? 1024 : remaining;

                // 3. Set up framing flags based on whether this is the final chunk
                int is_final_fragment = (pss->ptr + chunk_size >= pss->len);

                // 4. Perform the SINGLE allowed lws_write() call for this callback event
                // Note: lws_write expects the pointer to start AFTER the LWS_PRE padding
                int n = lws_write(wsi, &pss->buffer[LWS_PRE + pss->ptr], chunk_size, LWS_WRITE_TEXT);

                if (n < 0) {
                    lwsl_err("ERROR %d writing to ws socket\n", n);
                    return -1; // Closes the connection cleanly
                }

                // 5. Advance our read pointer by the number of bytes successfully written
                pss->ptr += chunk_size;

                // 6. If we have more data left, request another writeable callback immediately
                if (pss->ptr < pss->len) {
                    lws_callback_on_writable(wsi);
                } else {
                    // Clean up buffer memory if sending is completely finished
                    free(pss->buffer);
                    pss->buffer = NULL;
                }
                break;
            }

        case LWS_CALLBACK_CLIENT_RECEIVE:
            {
                Assert(len <= 4096);

                char buf[4096];

                memcpy(buf, in, len);
                buf[len] = '\0';

                LogInfo("Trade RESPONSE %d '%s'", (int)len, buf);

                yyjson_doc *doc =
                    yyjson_read(buf, len, 0);

                if (!doc)
                {
                    LogError("Couldn't parse Binance trade response");
                    break;
                }
                yyjson_val *root =
                    yyjson_doc_get_root(doc);

                /*
     * User Data Stream:
     *
     * {
     *     "subscriptionId": 0,
     *     "event": {
     *         "e": "executionReport",
     *         ...
     *     }
     * }
     */
                yyjson_val *event =
                    yyjson_obj_get(root, "event");

                if (event)
                {
                    yyjson_val *eventType =
                        yyjson_obj_get(event, "e");

                    if (eventType &&
                        strcmp(
                            yyjson_get_str(eventType),
                            "executionReport"
                        ) == 0)
                    {
                        UpdateOrderFromUserData(
                            pss->state,
                            event
                        );
                    }
               }
               else
            {
                    /*
         * Normal WebSocket API response:
         *
         * {
         *     "id": "...",
         *     "status": 200,
         *     "result": {...}
         * }
         */
                    yyjson_val *rpcStatus =
                        yyjson_obj_get(root, "status");

                    yyjson_val *result =
                        yyjson_obj_get(root, "result");

                    if (rpcStatus && result &&
                        yyjson_get_int(rpcStatus) == 200)
                    {
                        UpdateOrderFromBinance(
                            pss->state,
                            result
                        );
                    }
                } 

                yyjson_doc_free(doc);

                break;
            } 

        case LWS_CALLBACK_CLOSED:
            {
                // Clean up memory if the client disconnects before transmission completes
                if (pss && pss->buffer) {
                    free(pss->buffer);
                    pss->buffer = NULL;
                }
                break;
            }

        default:
            break;
    }

    return 0;
}

// Helper function to trigger a message send from your application logic
void
queue_message_to_send(struct lws *wsi,
                      struct per_session_data__minimal *pss,
                      const char *message)
{
    size_t msg_len = strlen(message);
    pss->len = msg_len;
    pss->ptr = 0;

    // CRITICAL: Libwebsockets requires LWS_PRE bytes of free padding BEFORE the payload data
    pss->buffer = (unsigned char *)malloc(LWS_PRE + msg_len);
    
    // Copy your payload into the safe region after the pre-padding space
    memcpy(&pss->buffer[LWS_PRE], message, msg_len);

    // Tell the lws event loop that this connection wants to write data
    lws_callback_on_writable(wsi);
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
                LogInfo("Failed str to float conversion\n");
            }
            if (j == 0)
            {
                q.price = f;
            }
            else
            {
                q.quantity = f;
            }
            // LogInfo("q %d, %f\n", (int)j, f);
        }

        quotes[idx] = q;
    }
}

bool
IsEventComplete(char *input)
{
    yyjson_doc *doc = yyjson_read(input, StringLength(input), 0);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *e = yyjson_obj_get(root, "e");
    if (e == NULL)
    {
        yyjson_doc_free(doc);
        return false;
    }
    else
    {
        yyjson_doc_free(doc);
        return true;
    }
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
    LogInfo("event type is %s\n", yyjson_get_str(e));
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
    LogInfo(
        "MEB=%p buffer=%p size=%u write=%u",
        (void *)marketEventsBuffer,
        (void *)marketEventsBuffer->buffer,
        marketEventsBuffer->size,
        marketEventsBuffer->currentWriteIndex
    );
    marketEventsBuffer->buffer[marketEventsBuffer->currentWriteIndex] = marketEvent;
    LogInfo("Buffered event U %lu at index %u\n",
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
    LogInfo("Buffered event at index %u\n",
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
                    LogInfo("removing ask at posn %d\n", removeAt);
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
                    LogInfo("inserting ask at posn %d\n", insertAt);
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
                        LogInfo("inserting hole at the end\n");
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
                    LogInfo("removing bid at posn %d\n", removeAt);
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
                    LogInfo("inserting bid at posn %d\n", insertAt);
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
                        LogInfo("inserting hole at the end\n");
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
LoadBufferAndApplyEvent(Market_event marketEvent, State *state, char *input)
{
    yyjson_doc *doc = yyjson_read(input, StringLength(input), 0);
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



size_t
writeDataBinanceOrder(void *buffer, size_t size, size_t nmemb, void *userp)
{
    (void) buffer;
    (void) userp;
    LogInfo("returned binance order\n");
    size_t realsize = size * nmemb;
    return realsize;
}

/* http call */
bool BinanceMakeOrder(char *body) {
    CURL *curl = curl_easy_init(); // Fresh handle
    /* NOTE: THIS LINE BELOW IS IMP FOR THE CALL TO RETURN */
    curl_easy_reset(curl);
    LogInfo("initialized curl \n");
    if(!curl) return false;
    curl_easy_reset(curl);

    char signed_body[2048];
    char signature[65];
    struct curl_slist *headers = NULL;

    char key_header[128];
    LogInfo(key_header, sizeof(key_header), "X-MBX-APIKEY: %s", getenv("API_KEY_SUB"));
    headers = curl_slist_append(headers, key_header);

    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    /* NOTE: THIS LINE BELOW IS IMP FOR THE CALL TO RETURN */
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, 0L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 2L);
    // curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeDataBinanceOrder);

    generate_signature(body, getenv("API_SECRET_SUB"), signature);
    LogInfo(signed_body, sizeof(signed_body), "%s&signature=%s", body, signature);

    char tradeUrl[1024];
    StringCpy(tradeUrl, (char *)TRADE_URL);
    StringCat(tradeUrl, signed_body);
    LogInfo("trade url is %s\n", tradeUrl);

    // Be careful here: Ensure tradeUrl + signed_body < 1024
    curl_easy_setopt(curl, CURLOPT_URL, tradeUrl);

    CURLcode result = curl_easy_perform(curl);
    LogInfo("initialized curl3 \n");
    if (result == CURLE_OK) {
        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
        if (http_code == 200) {
            LogInfo("Success! Order placed.\n");
            return true;
        }
        LogInfo("API Error! HTTP Status: %ld\n", http_code);
    } else {
        LogInfo("Transfer failed: %s\n", curl_easy_strerror(result));
    }

    // 3. Cleanup headers immediately
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    return false;
}

int
CallbackBinance(struct lws *wsi,
                enum lws_callback_reasons reason,
                void *user, void *in, size_t len)
{
    (void) wsi;
    (void) user;
    (void) in;
    (void) len;
    switch (reason)
    {
        case LWS_CALLBACK_CLIENT_ESTABLISHED:
            LogInfo("callback_binance: LWS_CALLBACK_CLIENT_ESTABLISHED\n");
            break;

        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
            LogInfo("LWS_CALLBACK_CLIENT_CONNECTION_ERROR\n");
            break;

        case LWS_CALLBACK_CLOSED:
            LogInfo("LWS_CALLBACK_CLOSED\n");
            break;

        case LWS_CALLBACK_CLIENT_RECEIVE:
            {
                ((char *)in)[len] = '\0';
                State *state = ((State *)user);
                Assert(len <= 4096 * 10)
                char buf[4096 * 10];
                memcpy(buf, in, len);
                buf[len] = '\0';
                LogInfo("rx %d '%s'\n", (int)len, buf);
                Market_event marketEvent = {};
                // TODO(Akhil): There's a double copy happening here,
                //              could be simpler.
                bool isComplete = IsEventComplete(buf);
                if (!isComplete)
                {
                    Assert(StringLength(state->event) + StringLength(buf) < 4096 * 10)
                    StringCat(state->event, buf);
                }
                else
                {
                    LoadBufferAndApplyEvent(marketEvent, (State *)user, buf);
                }

                isComplete = IsEventComplete(((State *)user)->event); 
                if (isComplete)
                {
                    LogInfo("NOT null anymore %s\n", ((State *)user)->event);
                    LoadBufferAndApplyEvent(marketEvent, (State *)user, buf);
                    memset(state->event, 0, sizeof(state->event));
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
    trade->id = (uint64)yyjson_get_int(id);
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
        state->sellPressureParent += trade->quantity;
        LogInfo("Sell pressure added to %f\n", state->sellPressure);
    }
    else
    {
        state->buyPressure += trade->quantity;
        state->buyPressureParent += trade->quantity;
        LogInfo("Buy pressure added to %f\n", state->buyPressure);
    }
    yyjson_doc_free(doc);
}

int
CallbackBinanceTradeStream(struct lws *wsi,
                enum lws_callback_reasons reason,
                void *user, void *in, size_t len)
{
    (void) wsi;
    switch (reason)
    {
        case LWS_CALLBACK_CLIENT_ESTABLISHED:
            LogInfo("callback_binance: LWS_CALLBACK_CLIENT_ESTABLISHED\n");
            break;

        case LWS_CALLBACK_CLOSED:
            LogInfo("LWS_CALLBACK_CLOSED\n");
            break;

        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
            LogInfo("LWS_CALLBACK_CLIENT_CONNECTION_ERROR\n");
            break;

        case LWS_CALLBACK_CLIENT_RECEIVE:
            {
                State *state = ((State *)user);
                state->lastTradeTime = time(NULL);
                Assert(len <= 4096)
                char buf[4096];
                memcpy(buf, in, len);
                buf[len] = '\0';
                // ((char *)in)[len] = '\0';
                LogInfo("rx Trade %d '%s'\n", (int)len, buf);
                Position position = state->position;
                Trade_event tradeEvent = {};
                bool isComplete = IsEventComplete(buf);
                if (!isComplete)
                {
                    Assert(StringLength(state->event) + StringLength(buf) < 4096)
                    StringCat(state->event, buf);
                }
                else
                {
                    LoadTradeEvent(&tradeEvent, buf, state);
                    BufferTradeEvent(tradeEvent, &state->TradeEventsBuffer);
                }

                isComplete = IsEventComplete(state->event); 
                if (isComplete)
                {
                    LogInfo("NOT null anymore %s\n", state->event);
                    LoadTradeEvent(&tradeEvent, buf, state);
                    BufferTradeEvent(tradeEvent, &state->TradeEventsBuffer);
                    memset(state->event, 0, sizeof(state->event));
                }

                real64 buyPressure = state->buyPressure;
                real64 sellPressure = state->sellPressure;
                real64 lastPrice = state->
                    TradeEventsBuffer.
                    buffer[MAX_EVENTS - 1].price;

                LogInfo("last price is %f\n", lastPrice);
                if (!(state->isPriceTaken) || state->startPrice == 0.0)
                {
                    LogInfo("setting the start price\n");
                    state->startPrice = lastPrice;
                    state->startPriceParent = lastPrice;
                    state->isPriceTaken = true;
                }

                if (!state->isOpen &&
                    lastPrice != 0.0)
                {
                    timespec endTime;
                    clock_gettime(CLOCK_MONOTONIC_RAW, &endTime);
                    real64 timeElapsedMS = XtimeElapsedMS(
                        state->lastTime,
                        endTime
                    );

                    real64 timeElapsedMSParent = XtimeElapsedMS(
                        state->lastTimeParent,
                        endTime
                    );
                    LogInfo("refresh Times are %f - %f\n", state->timeToRefresh, state->timeToRefreshParent);
                    LogInfo("Times are %f - %f\n", timeElapsedMS, timeElapsedMSParent);

                    if (timeElapsedMS > ((State *)user)->timeToRefresh)
                    {
                        state->startPrice = lastPrice;
                        state->lastTime = endTime;
                        state->buyPressure = 0.0;
                        state->sellPressure = 0.0;
                    }

                    if (timeElapsedMSParent > state->timeToRefreshParent)
                    {
                        LogInfo("Refreshing Parent\n");
                        state->startPriceParent   = lastPrice;
                        state->lastTimeParent     = endTime;
                        state->buyPressureParent  = 0.0;
                        state->sellPressureParent = 0.0;
                    }

                    LogInfo("start price is %f\n", state->startPrice);

                    if (abs((lastPrice - state->startPrice)) < 1.5)
                    {
                        LogInfo("Guilty! There is no price movement\n");
                    }
                    else
                    {
                        
                        if (lastPrice > (state->startPrice))
                        {
                            if (buyPressure < 1.1 * sellPressure)
                            {
                                LogInfo("Guilty! Not enough pressure on buy side\n");
                            }
                            else if (timeElapsedMS < 4 * 55 * 60 * 1000)
                            {
                                LogInfo("Guilty! Too fast, need real slow and steady!\n");
                            }
                            else if (lastPrice <= state->startPriceParent ||
                                state->buyPressureParent < state->sellPressureParent)
                            {
                                LogInfo("Guilty! Parent has done work in the opposite/choppy direction\n");
                                LogInfo("%f-%f, %f-%f\n", lastPrice, state->startPriceParent,
                                        state->buyPressureParent,
                                        state->sellPressureParent);
                            }
                            else
                            {
                                state->shouldPlaceOrder = true;
                                state->orderType = OPENBUY;
                            }
                        }
                        else
                        {
                            if (sellPressure < 1.1 * buyPressure)
                            {
                                LogInfo("Guilty! Not enough pressure on sell side\n");
                            }
                            else if (timeElapsedMS < 4 * 55 * 60 * 1000)
                            {
                                LogInfo("Guilty! Too fast, need real slow and steady!\n");
                            }
                            else if (lastPrice >= state->startPriceParent ||
                                state->buyPressureParent > state->sellPressureParent)
                            {
                                LogInfo( "Guilty! Parent has done work in the opposite/choppy direction\n");
                                LogInfo("%f-%f, %f-%f\n", lastPrice,
                                                         state->startPriceParent,
                                                         state->buyPressureParent,
                                                         state->sellPressureParent);
                            }
                            else
                            {
                                state->shouldPlaceOrder = true;
                                state->orderType = OPENSELL;
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
                    LogInfo("position is open, remaining time %f\n",
                           ((State *)user)->timeToClose - timeElapsedMS);

                    if(timeElapsedMS < state->timeToClose &&
                        ((posnType == LONG && ((lastPrice - state->startPrice) * position.qty) > -0.35) ||
                        (posnType == SHORT && ((lastPrice - state->startPrice) * position.qty) > -0.35))) 
                    {
                        LogInfo("Guilty! No need to close, time not out and loss in check\n");
                    }
                    else if(posnType == LONG &&
                            (buyPressure > 2 * sellPressure && 
                            ((state->startPrice - lastPrice) > 0.5)) &&
                            (state->timeToClose * 2 < MAX_TIME_PERIOD)) 
                    {
                        LogInfo("Guilty! No need to close, long pressure not reversed, so Load up!\n");
                        LogInfo("Loading the position at lastPrice %f\n", lastPrice);
                        state->shouldPlaceOrder = true;
                        state->orderType = LOADBUY;
                    }
                    else if(posnType == SHORT &&
                            (sellPressure > 2 * buyPressure && 
                            ((state->startPrice - lastPrice) > 0.5)) &&
                            (state->timeToClose * 2 < MAX_TIME_PERIOD)) 
                    {
                        LogInfo("Guilty! No need to close, short pressure not reversed, so Load up!\n");
                        LogInfo("Loading the position at lastPrice %f\n", lastPrice);
                        state->shouldPlaceOrder = true;
                        state->orderType = LOADSELL;
                    }
                    else // will only close now when the pressure's have reversed.
                    {
                        LogInfo("closing the position at lastPrice %f\n", lastPrice);
                        if (posnType == LONG)
                        {
                            state->shouldPlaceOrder = true;
                            state->orderType = CLOSELONG;
                        }
                        else if (posnType == SHORT)
                        {
                            state->shouldPlaceOrder = true;
                            state->orderType = CLOSESHORT;
                        }
                    }
                }
                break;
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
        "binance-trade-stream",
        CallbackBinanceTradeStream,
        0,
        1024,
    },
    {
        "binance-trade",
        CallbackBinanceTrade,
        sizeof(struct per_session_data__minimal),
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

    LogInfo("in write call back, size is %zu, resp is %s\n", realsize, snapshot->resp);
    return realsize;
}


// NOTE(Akhil): why void? can't the load fail? json wrong?
void
SetOrderBook(State *state)
{
    Order_book *OrderBook = &state->OrderBook;
    Snapshot *snapshot = &state->snapshot;
    char *input = (char *)snapshot->resp;
    yyjson_doc *doc = yyjson_read(input, StringLength(input), 0);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *id = yyjson_obj_get(root, "lastUpdateId");
    uint64 lastUpdateId = (uint64)yyjson_get_int(id);
    LogInfo("Last update id is %lu\n", lastUpdateId);
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
    LogInfo("BIDS===================\n");
    quote *bids = OrderBook->bids;
    quote *asks = OrderBook->asks;
    for (int i = 0; i < MAX_LEVELS; i++)
    {
        LogInfo("Price: %f, quantity: %f\n", bids[i].price, bids[i].quantity);
    }

    LogInfo("ASKS===================\n");
    for (int i = 0; i < MAX_LEVELS; i++)
    {
        LogInfo("Price: %f, quantity: %f\n", asks[i].price, asks[i].quantity);
    }
}

void
PrintTradeState(State *state)
{
    LogInfo("FORCES===================\n");
    LogInfo("START PRICE %f\n", state->startPrice); 
    LogInfo("SELL PRESSURE %f\n", state->sellPressure); 
    LogInfo("BUY PRESSURE %f\n", state->buyPressure); 
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
        LogInfo("Compare: lastId %lu , firstId %lu, and lastUpdateId %lu\n",
               lastId, firstId, lastUpdateId);
        if (lastId <= lastUpdateId)
        {
            LogInfo("Continuing\n");
            continue; // Ignore.
        }
        else if ((firstId - lastUpdateId) == 1)
        {
            LogInfo("Applying the event\n");
            ApplyEvent(event, OrderBook);
            lastUpdateId = lastId;
            applied = true;
            state->startPrice = (OrderBook->asks)[0].price;
            clock_gettime(CLOCK_MONOTONIC_RAW, &(state->lastTime));
        }
        else
        {
            // Missed some events, rework the entire snapshot.
            LogInfo("Rework the snapshot\n");
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

quote
getBestQuote(Order_book *orderBook)
{
    return orderBook->bids[0];
}



int
sendOrder(Order *order, Channel *tradeChannel)
{
    char priceStr[32];
    char qtyStr[32];
    snprintf(priceStr, sizeof(priceStr), "%.2f", order->price);
    snprintf(qtyStr, sizeof(qtyStr), "%.2f", order->qty);
    char uuidStr[37];
    generateUUID(uuidStr);
    strcpy(order->id, uuidStr);
    generateUUID(uuidStr);
    /* send the order through wsi instance */
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    // Set root["name"] and root["star"]
    yyjson_mut_obj_add_str(doc, root, "id", uuidStr);
    yyjson_mut_obj_add_str(doc, root, "method", "order.place");
    yyjson_mut_val *params = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, params, "apiKey", getenv("API_KEY_SUB"));
    yyjson_mut_obj_add_str(doc, params, "newClientOrderId", order->id);
    yyjson_mut_obj_add_str(doc, params, "price", priceStr);
    // yyjson_mut_obj_add_float(doc, params, "price", order->price);
    yyjson_mut_obj_add_str(doc, params, "quantity", qtyStr);
    // char qtyStr[32]; /* to match teh %.2f in body formatting below */
    // LogInfo(qtyStr, sizeof(qtyStr), "%.2f", order->qty);
    // yyjson_mut_obj_add_val(doc, params, "quantity", yyjson_mut_raw(doc, qtyStr)); 
    yyjson_mut_obj_add_str(doc, params, "side", OrderSideString[order->side]);
    /* NOTE(AKHIL): for the signature to pass, the params should be sorted
     *              alphabetically, and the price and quantities should be 
     *              same in query string and params json to the decimal point */
    uint64 timestamp = BinanceTimestamp();
    char body[1024];
    snprintf(body,
             sizeof(body),
             "apiKey=%s&newClientOrderId=%s&price=%s&quantity=%s&side=%s&symbol=%s&timeInForce=%s&timestamp=%lu&type=%s",
             getenv("API_KEY_SUB"),
             order->id,
             priceStr,
             qtyStr,
             OrderSideString[order->side],
             order->coin,
             "GTC",
             timestamp,
             OrderTypeString[order->type]); 
    LogInfo("body is %s\n", body);
    char signature[2048];
    generate_signature(body, getenv("API_SECRET_SUB"), signature);
    yyjson_mut_obj_add_str(doc, params, "signature", signature);
    yyjson_mut_obj_add_str(doc, params, "symbol", order->coin);
    yyjson_mut_obj_add_str(doc, params, "timeInForce", "GTC");
    yyjson_mut_obj_add_int(doc, params, "timestamp", timestamp);
    yyjson_mut_obj_add_str(doc, params, "type", OrderTypeString[order->type]);
    yyjson_mut_obj_add_val(doc, root, "params", params);
    char *json = yyjson_mut_write(doc, 0, NULL);
    LogInfo("json is %s\n", json);
    LogInfo("WRITING==============\n");
    // char buf[LWS_PRE + StringLength(json)];
    // memcpy(&buf[LWS_PRE], json, StringLength(json));
    // lws_write(lwsTrade, (unsigned char *)&buf[LWS_PRE], StringLength(json), LWS_WRITE_TEXT);
    ChannelPut(tradeChannel, json);
    yyjson_mut_doc_free(doc);
    return 0;
}

int
cancelOrder(Order *order, Channel *tradeChannel)
{
    char uuidStr[37];
    /* cancel that order */
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    generateUUID(uuidStr);
    // Set root["name"] and root["star"]
    yyjson_mut_obj_add_str(doc, root, "id", uuidStr);
    yyjson_mut_obj_add_str(doc, root, "method", "order.cancel");
    yyjson_mut_val *params = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, params, "apiKey", getenv("API_KEY_SUB"));
    yyjson_mut_obj_add_str(doc, params, "origClientOrderId", order->id);
    uint64 timestamp = BinanceTimestamp();
    char body[1024];
    snprintf(body,
             sizeof(body),
             "apiKey=%s&origClientOrderId=%s&symbol=%s&timestamp=%lu",
             getenv("API_KEY_SUB"),
             order->id,
             order->coin,
             timestamp);
    LogInfo("body is %s\n", body);
    char signature[2048];
    generate_signature(body, getenv("API_SECRET_SUB"), signature);
    yyjson_mut_obj_add_str(doc, params, "signature", signature);
    yyjson_mut_obj_add_str(doc, params, "symbol", order->coin);
    yyjson_mut_obj_add_int(doc, params, "timestamp", timestamp);
    yyjson_mut_obj_add_val(doc, root, "params", params);
    char *json = yyjson_mut_write(doc, 0, NULL);
    LogInfo("json is %s\n", json);
    LogInfo("WRITING CANCEL==============\n");
    // char bufCancel[LWS_PRE + StringLength(json)];
    // memcpy(&bufCancel[LWS_PRE], json, StringLength(json));
    // lws_write(lwsTrade, (unsigned char *)&bufCancel[LWS_PRE], StringLength(json), LWS_WRITE_TEXT);
    ChannelPut(tradeChannel, json);
    yyjson_mut_doc_free(doc);
    return 0;
}

void
cancelAllOrders(State *state, Channel *tradeChannel)
{
    for (int i = 0; i < MAX_ORDERS; i++)
    {
        if (0 != strcmp(state->orders[i].id, ""))
        {
            cancelOrder(&state->orders[i], tradeChannel);
            state->currOrderIndex--;
        }
    }
}

static void *
tradeThread(void *arg)
{
    TradeThreadArgs *args = (TradeThreadArgs *)arg;

    while (1)
    {
        /*
         * Only take a new message if pss isn't
         * already holding one.
         */
        if (args->pss->buffer == NULL)
        {
            char *message =
                (char *)ChannelTake(args->channel);
            // LogInfo("Taken message is %s\n", message);

            if (message != NULL)
            {
                size_t len = strlen(message);

                args->pss->buffer =
                    (unsigned char *)malloc(LWS_PRE + len);

                memcpy(
                    &args->pss->buffer[LWS_PRE],
                    message,
                    len
                );

                args->pss->len = len;
                args->pss->ptr = 0;

                free(message);

                lws_callback_on_writable(args->wsi);
            }
        }

        /*
         * This is what actually drives LWS and
         * eventually invokes CLIENT_WRITEABLE.
         * this actually also drives the events for
         * order book, same context eh
         */
        lws_service(args->context, 0);
    }

    return NULL;
}

void
DashboardRender(
    Position *position,
    Order *orders,
    int orderCount
)
{
    /*
     * Clear terminal and move cursor to top.
     */
    printf("\033[2J\033[H");

    printf("============================================================\n");
    printf("                       TRADING BOT                          \n");
    printf("============================================================\n");

    /*
     * Position
     */
    printf("\nPOSITION\n");
    printf("------------------------------------------------------------\n");

    if (position == NULL || position->qty == 0)
    {
        printf("No open position\n");
    }
    else
    {
        char timeStr[100];
        formatMSTimestamp(position->timestamp, timeStr, sizeof(timeStr));
        printf("Symbol       : %s\n", position->symbol);
        printf("Quantity     : %.4f\n", position->qty);
        printf("Entry Price  : %.4f\n", position->price);
        printf("LTP          : %.4f\n", position->ltp);
        printf("PnL          : %.4f\n", position->pnl);
        printf("Last updated :  %s\n", timeStr);
    }

    /*
     * Active orders
     */
    printf("\nACTIVE ORDERS\n");
    printf("------------------------------------------------------------\n");

    printf(
        "%-20s %-8s %-10s %-10s %-12s %-10s\n",
        "ID",
        "SIDE",
        "QTY",
        "PRICE",
        "TYPE",
        "STATUS"
    );

    for (int i = 0; i < orderCount; i++)
    {
        Order *order = &orders[i];

        if (order->status == CANCELLED)
            continue;

        printf(
            "%-20.20s %-8s %-10.4f %-10.4f %-12s %-10s\n",
            order->id,
            SideString(order->side),
            order->qty,
            order->price,
            OrderTypeString[order->type],
            OrderStatusString(order->status)
        );
    }

    /*
     * Current time
     */
    printf("\n------------------------------------------------------------\n");
    printf("Updated: ");

    time_t now = time(NULL);
    struct tm tm_now;

    localtime_r(&now, &tm_now);

    char buf[32];

    strftime(
        buf,
        sizeof(buf),
        "%Y-%m-%d %H:%M:%S",
        &tm_now
    );

    printf("%s\n", buf);

    printf("============================================================\n");

    fflush(stdout);
}

int
main()
{
    LogInit(LOG_ERROR);
    Channel tradeChannel;
    ChannelInit(&tradeChannel);
    CURLcode res = curl_global_init(CURL_GLOBAL_ALL);
    if (res != CURLE_OK) {
        LogInfo("curl setup failed, abort!");
        return -1;
    }

    CURL *curl = curl_easy_init(); // Fresh handle
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);
    
    FILE *outputFile = fopen("outputLive.csv", "w");
    setbuf(outputFile, NULL); // Disables buffering completely
    if (outputFile == NULL)
    {
        LogInfo("Couldn't open file\n");
        return -1;
    }

    fputs("id, Timestamp, symbol, side, price, qty, curr_value, usdt_after_fee, qty_after_fee, pnl\n", outputFile);
    State state = {};
    StringCpy(state.event, (char *)"");
    state.isSnapshot = false;
    state.isPriceTaken = false;
    state.shouldPlaceOrder = false;
    state.MarketEventsBuffer.size = MAX_EVENTS;
    state.MarketEventsBuffer.currentWriteIndex = 0;
    state.TradeEventsBuffer.size = MAX_EVENTS;
    state.TradeEventsBuffer.currentWriteIndex = 0;
    state.buyPressure = 0.0;
    state.sellPressure = 0.0;
    state.buyPressureParent = 0.0;
    state.sellPressureParent = 0.0;
    state.timeToRefresh = 4 * 60 * 60 * 1000;
    state.timeToRefreshParent = 8 * 60 * 60 * 1000;
    Position position = {};
    position.symbol = (char *)"SOLUSDT";
    Wallet wallet = {};
    wallet.usdt = 71.62;
    wallet.coin = 16.02;
    state.wallet = wallet;
    state.position = position;
    state.outputFile = outputFile;
    timespec endTime;
    clock_gettime(CLOCK_MONOTONIC_RAW, &endTime);
    state.lastTime = endTime;
    state.currOrderIndex = -1;
    state.SL = 0.04;
    // | LLL_DEBUG
    // lws_set_log_level(LLL_ERR | LLL_WARN | LLL_NOTICE | LLL_INFO, NULL); 
    LogInfo("running\n");
    char address[1024];
    StringCpy(address, (char *)MARKET_BASE_ENDP);
    StringCat(address, (char *)STREAM_PATH);
    LogInfo("address is %s\n", address);
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
        LogInfo("Couldn't create context\n");
        return -1;
    }
    /* connect the websocket to binance orderbook */
    
    struct lws_client_connect_info ccinfo = {};
    ccinfo.context = context;
    ccinfo.address = MARKET_BASE_ENDP;
    ccinfo.port = port;
    ccinfo.ssl_connection = 1;
    ccinfo.path = STREAM_PATH;
    ccinfo.host = ccinfo.address;
    ccinfo.origin = ccinfo.address;
    ccinfo.ssl_connection = LCCSCF_USE_SSL;
    ccinfo.ietf_version_or_minus_one = -1;
    ccinfo.protocol = "binance";
    ccinfo.userdata = (void *)&state;
    struct lws *lws = lws_client_connect_via_info(&ccinfo);
    if (lws == NULL)
    {
        LogInfo("Connection failed\n");
        return -1;
    }

    /* connect the websocket to binance trade api*/
    struct lws_client_connect_info ccinfoTrade = {};
    ccinfoTrade.context = context;
    ccinfoTrade.address = TRADE_BASE_ENDP;
    ccinfoTrade.port = port;
    ccinfoTrade.ssl_connection = 1;
    ccinfoTrade.path = TRADE_PATH;
    ccinfoTrade.host = ccinfoTrade.address;
    ccinfoTrade.origin = ccinfoTrade.address;
    ccinfoTrade.ssl_connection = LCCSCF_USE_SSL;
    ccinfoTrade.ietf_version_or_minus_one = -1;
    ccinfoTrade.protocol = "binance-trade";
    struct per_session_data__minimal pss = {}; 
    pss.state = &state;
    pss.channel = &tradeChannel;
    pss.buffer = NULL;
    ccinfoTrade.userdata = (void *)&pss;
    struct lws *lwsTrade = lws_client_connect_via_info(&ccinfoTrade);
    if (lwsTrade == NULL)
    {
        LogInfo("Connection failed\n");
        return -1;
    }
    uint16 loopcount = 0;
    TradeThreadArgs tradeArgs = {
        .channel = &tradeChannel,
        .context = context,
        .wsi = lwsTrade,
        .pss = &pss
    };

    pthread_t tradeThreadId;

    pthread_create(
        &tradeThreadId,
        NULL,
        tradeThread,
        &tradeArgs
    ); 

    while(1)
    {
        if (state.MarketEventsBuffer.currentWriteIndex > 0 &&
            !state.isSnapshot) {
            LogInfo("Checking for snapshot...\n");
            curl_easy_setopt(curl, CURLOPT_URL, SNAPSHOT_URL);
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&state.snapshot);
            CURLcode result = curl_easy_perform(curl);
            if (result != CURLE_OK) {
                LogInfo("curl call failed!, %s abort!\n", curl_easy_strerror(result));
            }
            char *input = (char *)state.snapshot.resp;
            yyjson_doc *doc = yyjson_read(input, StringLength(input), 0);
            yyjson_val *root = yyjson_doc_get_root(doc);
            yyjson_val *id = yyjson_obj_get(root, "lastUpdateId");
            uint64 lastUpdateId = (uint64)yyjson_get_int(id);
            LogInfo("Last update id is %lu\n", lastUpdateId);
            Market_event firstEvent = state.MarketEventsBuffer.buffer[0];
            LogInfo("first update id is %lu\n", firstEvent.U);
            LogInfo("Compare: lastUpdateId %lu with first event id %lu\n",
                   lastUpdateId, firstEvent.U);
            LogInfo("current Write inDex is %u\n",
                   state.MarketEventsBuffer.currentWriteIndex);
            if (lastUpdateId > firstEvent.U) {
                LogInfo("LastUpdateId %lu > the first buffered event id!", lastUpdateId);
                state.isSnapshot = true;
            }
        }
        else if (state.isSnapshot &&
                 !state.AreEventsApplied)
        {
            SetOrderBook(&state);
            LogInfo("Order book id is %lu\n", state.OrderBook.lastUpdateId);
            // discard/ignore the buffered events where the id < snapshot id
            // apply the buffered events to the order book
            IgnoreAndApplyEvents(&state);
        }

        if (loopcount % 1000 == 0) {
            malloc_trim(0); 
            loopcount = 0;
        }

        /* on each tick, check if there is a position open,
           if yes, check the loss, if it's >= SL, take the loss
           and cancel the other order,else if its in profit,
           wait till it hits the other order.

           on the other hand, if there is no position, send
           buy and sell orders, and go back to step 1.

           The point of refresh is on a pair, we refresh the 
           pair to get it higher up the queue on the exchange */ 
        
        timespec endTime;
        clock_gettime(CLOCK_MONOTONIC_RAW, &endTime);
        real64 timeElapsedMS = XtimeElapsedMS(
            state.lastTimeDashboard,
            endTime
        );
        if (timeElapsedMS > 250)
        {
            state.lastTimeDashboard = endTime;
            DashboardRender(
                &state.position,
                state.orders,
                state.currOrderIndex + 1
            );
        }

        if (state.position.qty == 0)
        {
            /* check if refresh and
               create the pair of orders with the target spread. */
            real64 timeElapsedMS = XtimeElapsedMS(
                state.lastTime,
                endTime
            );
            // LogInfo("time elapsed is %f\n", timeElapsedMS);
            if (timeElapsedMS > MIN_REFRESH_TIME)
            {
                state.lastTime = endTime;
                LogInfo("TIME ELAPSED=====\n");
                cancelAllOrders(&state, &tradeChannel);
                // createNewPair(state.orders);
                Order buyOrder = {};
                Order sellOrder = {};
                buyOrder.timestamp = XgetTimestamp();
                sellOrder.timestamp = XgetTimestamp();
                buyOrder.coin = (char *)"SOLUSDT";
                buyOrder.side = BUY;
                buyOrder.type = LIMIT;
                buyOrder.qty = 0.1;
                buyOrder.price = state.OrderBook.bids[SPREAD_LEVEL].price; 
                buyOrder.status = PENDING; 
                sellOrder.coin = (char *)"SOLUSDT";
                sellOrder.side = SELL;
                sellOrder.type = LIMIT;
                sellOrder.qty = 0.1;
                sellOrder.price = state.OrderBook.asks[SPREAD_LEVEL].price;
                sellOrder.status = PENDING; 
                int res = sendOrder(&buyOrder, &tradeChannel);
                res = sendOrder(&sellOrder, &tradeChannel);
                state.orders[++state.currOrderIndex] = buyOrder;
                state.orders[++state.currOrderIndex] = sellOrder;
            }
        }
        else
        {
            LogInfo("CHECKING SL======\n");
            /* update the position's pnl and if >= sl, cancel 
               the other order */
            quote bestQ = getBestQuote(&state.OrderBook);
            real64 ltp = bestQ.price;
            state.position.ltp = ltp;
            state.position.pnl = (ltp - state.position.price) * state.position.qty;
            if (state.position.pnl < 0 && abs(state.position.pnl) >= state.SL)
            {
                LogInfo("SL HIT ======\n");
                /* close the position and cancel all orders */
                Order closeOrder = {};
                closeOrder.timestamp = XgetTimestamp();
                closeOrder.side = (state.position.qty > 0) ? SELL : BUY;
                closeOrder.type = MARKET;
                closeOrder.qty = state.position.qty;
                closeOrder.status = PENDING;
                strcpy(closeOrder.coin, state.position.symbol);
                int res = sendOrder(&closeOrder, &tradeChannel);
                if (res < 0) LogInfo("couldn't close order \n");
                cancelAllOrders(&state, &tradeChannel);
            }
        }

        // apply the event to the order book in the callback, if the OB is ready.
        // lws_service(context, 0);

        // PrintOrderBook(&state);
        // PrintTradeState(&state);
        loopcount++;
    }


    lws_context_destroy(context);
    return 0;
}

// if ((time(NULL) - state.lastTradeTime > 10)) {
        //     LogInfo("No data — reconnecting\n");
        //     fflush(stdout);
        //
        //     lws_context_destroy(context);
        //     context = lws_create_context(&info);
        //     ccinfoTrade.context = context;
        //     lwsTrade = lws_client_connect_via_info(&ccinfoTrade); 
        //     if (lwsTrade == NULL)
        //     {
        //         LogInfo("Connection failed\n");
        //         return -1;
        //     }
        //
        //     state.lastTradeTime = time(NULL);
        // } 
        //
        // if (state.shouldPlaceOrder)
        // {
        //     timespec endTime;
        //     clock_gettime(CLOCK_MONOTONIC_RAW, &endTime);
        //     real64 timeElapsedMS = XtimeElapsedMS(
        //         state.lastTime,
        //         endTime
        //     );
        //     real64 lastPrice = state.
        //             TradeEventsBuffer.
        //             buffer[MAX_EVENTS - 1].price;
        //     uint64 timestamp = BinanceTimestamp();
        //     char body[900];
        //     char time_str[32];
        //     if (state.orderType == OPENBUY)
        //     {
        //         LogInfo("opening the position after %f at lastPrice %f\n",
        //                timeElapsedMS,
        //                lastPrice);
        //         // make the order call.
        //         sLogInfo(body, "symbol=%s&side=%s&type=%s&quantity=%f&timestamp=%lu",
        //                 "SOLUSDT",
        //                 "BUY",
        //                 "MARKET",
        //                 0.1,
        //                 timestamp);
        //         LogInfo("body is %s, api key is %s\n", body, getenv("API_KEY"));
        //         bool res = BinanceMakeOrder(body);
        //         if (res)
        //         {
        //             Trade trade = {};
        //             real64 qty = 0.1;
        //             trade.qty = qty;
        //             trade.price = lastPrice;
        //             trade.side = BUY;
        //             trade.usdtAfterFee = (qty * lastPrice) * (1 - TRADE_FEE);
        //             trade.qtyAfterFee = trade.usdtAfterFee / lastPrice; 
        //             position.qty += trade.qtyAfterFee;
        //             position.price = lastPrice;
        //             wallet.coin += trade.qtyAfterFee;
        //             wallet.usdt -= qty * lastPrice;
        //             formatMSTimestamp(timestamp, time_str, sizeof(time_str));
        //             sLogInfo(body, "%lu, %s, %s, %s, %f, %f, %f, %f, %f, %f",
        //                     timestamp,
        //                     time_str,
        //                     "SOLUSDT",
        //                     "BUY",
        //                     lastPrice,
        //                     qty,
        //                     position.qty * position.price,
        //                     trade.usdtAfterFee,
        //                     trade.qtyAfterFee,
        //                     0.0);
        //             fputs(StringCat(body, "\n"), outputFile);
        //             state.posnType = LONG;
        //             state.timeToClose = timeElapsedMS;
        //             state.lastTime = endTime;
        //             state.isOpen = true;
        //             state.buyPressure = 0.0;
        //             state.sellPressure = 0.0;
        //         }
        //     }
        //     else if (state.orderType == OPENSELL)
        //     {
        //         LogInfo("opening the position after %f at lastPrice %f\n",
        //                timeElapsedMS,
        //                lastPrice);
        //         // make the order call.
        //         sLogInfo(body, "symbol=%s&side=%s&type=%s&quantity=%f&timestamp=%lu",
        //                 "SOLUSDT",
        //                 "SELL",
        //                 "MARKET",
        //                 0.1,
        //                 timestamp);
        //         LogInfo("body is %s, api key is %s\n", body, getenv("API_KEY"));
        //         bool res = BinanceMakeOrder(body);
        //         if (res)
        //         {
        //             Trade trade = {};
        //             real64 qty = -0.1;
        //             trade.qty = qty;
        //             trade.price = lastPrice;
        //             trade.side = SELL;
        //             trade.qtyAfterFee = (qty) * (1 - TRADE_FEE);
        //             trade.usdtAfterFee = trade.qtyAfterFee * lastPrice; 
        //             position.qty += trade.qtyAfterFee;
        //             position.price = lastPrice;
        //             wallet.coin += qty;
        //             wallet.usdt -= trade.qtyAfterFee * lastPrice;
        //             formatMSTimestamp(timestamp, time_str, sizeof(time_str));
        //             // make the order call.
        //             sLogInfo(body, "%lu, %s, %s, %s, %f, %f, %f, %f, %f, %f",
        //                     timestamp,
        //                     time_str,
        //                     "SOLUSDT",
        //                     "SELL",
        //                     lastPrice,
        //                     qty,
        //                     position.qty * position.price,
        //                     trade.usdtAfterFee,
        //                     trade.qtyAfterFee,
        //                     0.0);
        //             fputs(StringCat(body, "\n"), outputFile);
        //             state.posnType = SHORT;
        //             state.timeToClose = timeElapsedMS;
        //             state.lastTime = endTime;
        //             state.isOpen = true;
        //             state.buyPressure = 0.0;
        //             state.sellPressure = 0.0;
        //         }
        //     }
        //     else if (state.orderType == LOADBUY)
        //     {
        //         sLogInfo(body, "symbol=%s&side=%s&type=%s&quantity=%f&timestamp=%lu",
        //                 "SOLUSDT",
        //                 "SELL",
        //                 "MARKET",
        //                 0.1,
        //                 timestamp);
        //         LogInfo("body is %s, api key is %s\n", body, getenv("API_KEY"));
        //         if (BinanceMakeOrder(body))
        //         {
        //             Trade trade = {};
        //             real64 qty = 0.1;
        //             trade.qty = qty;
        //             trade.price = lastPrice;
        //             trade.side = BUY;
        //             trade.usdtAfterFee = (qty * lastPrice) * (1 - TRADE_FEE);
        //             trade.qtyAfterFee = trade.usdtAfterFee / lastPrice; 
        //             position.qty += trade.qtyAfterFee;
        //             position.price = lastPrice;
        //             wallet.coin += trade.qtyAfterFee; 
        //             wallet.usdt -= qty * lastPrice;
        //             formatMSTimestamp(timestamp, time_str, sizeof(time_str));
        //
        //             sLogInfo(body, "%lu, %s, %s, %s, %f, %f, %f, %f, %f, %f",
        //                     timestamp,
        //                     time_str,
        //                     "SOLUSDT",
        //                     "BUY",
        //                     lastPrice,
        //                     qty,
        //                     position.qty * position.price,
        //                     trade.usdtAfterFee,
        //                     trade.qtyAfterFee,
        //                     0.0);
        //             fputs(StringCat(body, "\n"), outputFile);
        //             state.startPrice = lastPrice;
        //             state.lastTime = endTime;
        //             state.buyPressure = 0.0;
        //             state.sellPressure = 0.0;
        //             state.timeToClose *= 2;
        //         }
        //     }
        //     else if (state.orderType == LOADSELL)
        //     {
        //         sLogInfo(body, "symbol=%s&side=%s&type=%s&quantity=%f&timestamp=%lu",
        //                 "SOLUSDT",
        //                 "SELL",
        //                 "MARKET",
        //                 0.1,
        //                 timestamp);
        //         LogInfo("body is %s, api key is %s\n", body, getenv("API_KEY"));
        //         if (BinanceMakeOrder(body))
        //         {
        //             Trade trade = {};
        //             real64 qty = -0.1;
        //             trade.qty = qty;
        //             trade.price = lastPrice;
        //             trade.side = SELL;
        //             trade.qtyAfterFee = (qty) * (1 - TRADE_FEE);
        //             trade.usdtAfterFee = trade.qtyAfterFee * lastPrice; 
        //             position.qty += trade.qtyAfterFee;
        //             position.price = lastPrice;
        //             wallet.coin += qty;
        //             wallet.usdt -= trade.qtyAfterFee * lastPrice; 
        //             formatMSTimestamp(timestamp, time_str, sizeof(time_str));
        //
        //             // make the order call.
        //             sLogInfo(body, "%lu, %s, %s, %s, %f, %f, %f, %f, %f, %f",
        //                     timestamp,
        //                     time_str,
        //                     "SOLUSDT",
        //                     "SELL",
        //                     lastPrice,
        //                     qty,
        //                     position.qty * position.price,
        //                     trade.usdtAfterFee,
        //                     trade.qtyAfterFee,
        //                     0.0); 
        //             fputs(StringCat(body, "\n"), outputFile);
        //             state.startPrice = lastPrice;
        //             state.lastTime = endTime;
        //             state.buyPressure = 0.0;
        //             state.sellPressure = 0.0;
        //             state.timeToClose *= 2;
        //         }
        //     }
        //     else if (state.orderType == CLOSELONG)
        //     {
        //         sLogInfo(body, "symbol=%s&side=%s&type=%s&quantity=%f&timestamp=%lu",
        //                 "SOLUSDT",
        //                 "SELL",
        //                 "MARKET",
        //                 0.1,
        //                 timestamp);
        //         if (BinanceMakeOrder(body))
        //         {
        //             Trade trade = {};
        //             real64 currPosValue = position.qty * lastPrice;
        //             real64 prevPosValue = position.qty * position.price;
        //             real64 qty = -position.qty;
        //             trade.qty = qty;
        //             trade.price = lastPrice;
        //             trade.side = SELL;
        //             trade.qtyAfterFee = (qty) * (1 - TRADE_FEE);
        //             trade.usdtAfterFee = trade.qtyAfterFee * lastPrice; 
        //             position.qty += trade.qtyAfterFee;
        //             position.price = lastPrice;
        //             wallet.coin += qty;
        //             wallet.usdt -= trade.qtyAfterFee * lastPrice; 
        //             formatMSTimestamp(timestamp, time_str, sizeof(time_str));
        //
        //             sLogInfo(body, "%lu, %s, %s, %s, %f, %f, %f, %f, %f, %f",
        //                     timestamp,
        //                     time_str,
        //                     "SOLUSDT",
        //                     "SELL",
        //                     lastPrice,
        //                     qty,
        //                     position.qty * position.price,
        //                     trade.usdtAfterFee,
        //                     trade.qtyAfterFee,
        //                     currPosValue - prevPosValue);
        //             fputs(StringCat(body, "\n"), outputFile);
        //             state.posnType = ZERO;
        //             state.isOpen = false;
        //             state.startPrice = lastPrice;
        //             state.lastTime = endTime;
        //             state.buyPressure = 0.0;
        //             state.sellPressure = 0.0;
        //         }
        //     }
        //     else if (state.orderType == CLOSESHORT)
        //     {
        //         sLogInfo(body, "symbol=%s&side=%s&type=%s&quantity=%f&timestamp=%lu",
        //                 "SOLUSDT",
        //                 "BUY",
        //                 "MARKET",
        //                 0.1,
        //                 timestamp);
        //         if (BinanceMakeOrder(body))
        //         {
        //             Trade trade = {};
        //             real64 currPosValue = position.qty * lastPrice;
        //             real64 prevPosValue = position.qty * position.price;
        //             real64 qty = -position.qty;
        //             trade.qty = qty;
        //             trade.price = lastPrice;
        //             trade.side = BUY;
        //             trade.usdtAfterFee = (qty * lastPrice) * (1 - TRADE_FEE);
        //             trade.qtyAfterFee = trade.usdtAfterFee / lastPrice; 
        //             position.qty += trade.qtyAfterFee;
        //             position.price = lastPrice;
        //             wallet.coin += trade.qtyAfterFee; 
        //             wallet.usdt -= qty * lastPrice;
        //             formatMSTimestamp(timestamp, time_str, sizeof(time_str));
        //
        //             sLogInfo(body, "%lu, %s, %s, %s, %f, %f, %f, %f, %f, %f",
        //                     timestamp,
        //                     time_str,
        //                     "SOLUSDT",
        //                     "BUY",
        //                     lastPrice,
        //                     qty,
        //                     position.qty * position.price,
        //                     trade.usdtAfterFee,
        //                     trade.qtyAfterFee,
        //                     currPosValue - prevPosValue);
        //             fputs(StringCat(body, "\n"), outputFile);
        //             state.posnType = ZERO;
        //             state.isOpen = false;
        //             state.startPrice = lastPrice;
        //             state.lastTime = endTime;
        //             state.buyPressure = 0.0;
        //             state.sellPressure = 0.0;
        //         }
        //     }
        //     state.shouldPlaceOrder = false;
        // }
// struct lws_protocols protocolTrade = {};
    // protocol.name = "binance-trade";
    // protocol.callback = CallbackBinanceTrade;
    // protocol.per_session_data_size = 256;
    //
    // struct lws_client_connect_info ccinfoTrade = {};
    // ccinfoTrade.context = context;
    // ccinfoTrade.address = MARKET_BASE_ENDP;
    // ccinfoTrade.port = port;
    // ccinfoTrade.ssl_connection = 1;
    // ccinfoTrade.path = TRADE_STREAM_PATH;
    // ccinfoTrade.host = ccinfoTrade.address;
    // ccinfoTrade.origin = ccinfoTrade.address;
    // ccinfoTrade.ssl_connection = LCCSCF_USE_SSL;
    // ccinfoTrade.ietf_version_or_minus_one = -1;
    // ccinfoTrade.protocol = "binance-trade";
    // ccinfoTrade.userdata = (void *)&state;
    // struct lws *lwsTrade = lws_client_connect_via_info(&ccinfoTrade);
    // if (lwsTrade == NULL)
    // {
    //     LogInfo("Connection failed\n");
    //     return -1;
    // }


