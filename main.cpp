#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <libwebsockets.h>
#include <yyjson.h>
#include <curl/curl.h>

#define ArrayCount(Array) (sizeof(Array) / sizeof(Array[0]))
#define Assert(Expression) if(!(Expression)) {*(int *)0 = 0;}
#define MARKET_BASE_ENDP "stream.binance.com"
#define STREAM_PATH "/ws/bnbbtc@depth"
#define MAX_LEVELS 10
#define MAX_EVENTS 10 
#define SNAPSHOT_URL "https://api.binance.com/api/v3/depth?symbol=BNBBTC&limit=10"

typedef uint32_t uint32;
typedef uint64_t uint64;
typedef float real32;
typedef double real64;
typedef uint16_t uint16;

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

typedef struct {
    char *resp;
    size_t size;
} Snapshot;

typedef struct quoteNode quoteNode;

struct quoteNode 
{
    quote q;
    quoteNode *next;
};

typedef struct
{
    uint64 lastUpdateId;
    quoteNode *asks; // 10 levels on each side.
    quoteNode *bids;
} Order_book;

typedef enum
{
    ASK,
    BID
} quoteType;

Market_events_buffer globalMarketEventsBuffer = {};
Snapshot globalSnapshot = {};
Order_book globalOrderBook = {};
bool globalAreEventsApplied = false;

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
AddLevelsToEvent(yyjson_val *val, quote quotes[], quoteType type)
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

void
AddLevelsToOB(yyjson_val *val, quoteNode *ptr)
{
    size_t idx;
    size_t max;
    yyjson_val *array;
    yyjson_arr_foreach(val, idx, max, array)
    {
        if (idx >= MAX_LEVELS) break; // only read the max levels.
        yyjson_val *a;
        quoteNode node = {};
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
                node.q.price = f;
            }
            else
            {
                node.q.quantity = f;
            }
            // printf("q %d, %f\n", (int)j, f);
        }


        if (idx == 0)
        {
            ptr = &node;
            ptr->next = NULL;
        }
        else
        {
            ptr->next = &node;
            node.next = NULL;
        }
    }
}

// TODO(Akhil): Make this handle incomplete inputs that are 
// later completed.
void
LoadMarketEvent(char *input, Market_event *event)
{
    yyjson_doc *doc = yyjson_read(input, StringLength(input), 0);
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
    AddLevelsToEvent(a, event->asks, ASK);
    AddLevelsToEvent(b, event->bids, BID);
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



void
ApplyEvent(Market_event event, Order_book *globalOrderBook)
{
    quote *eventAsks = event.asks;
    quote *eventBids = event.bids;
    quoteNode *OBAsks = globalOrderBook->asks;
    quoteNode *OBBids = globalOrderBook->bids;

    Assert(ArrayCount(event.asks) == MAX_LEVELS)
    for (int i = 0; i < MAX_LEVELS; i++)
    {
        quote eventAsk = eventAsks[i];
        quoteNode *insertAt;
        if (eventAsk.quantity == 0)
        {
            // remove from the linked list.
            quoteNode *ptr = OBAsks;
            quoteNode *prev = ptr;
            while (ptr != NULL)
            {
                if (ptr->q.price == eventAsk.price)
                {
                    prev->next = ptr->next;
                    break;
                }
                prev = ptr;
                ptr = ptr->next;
            }
        }
        else
        {
            quoteNode *ptr = OBAsks;
            quoteNode *prev = ptr;
            while (ptr != NULL)
            {
                if (ptr->q.price == eventAsk.price)
                {
                    insertAt = NULL;
                    ptr->q.quantity = eventAsk.quantity;
                    break;
                }
                else if (eventAsk.price < ptr->q.price)
                {
                    insertAt = prev;
                    prev->next = ptr->next;
                    ptr->next = NULL;
                    break;
                }

                prev = ptr;
                ptr = ptr->next;
            }

            if (insertAt != NULL)
            {
                quoteNode node = {};
                node.q = eventAsk;
                node.next = prev->next;
                prev->next = &node;
            }
        }
    }

    Assert(ArrayCount(event.asks) == MAX_LEVELS)
    for (int i = 0; i < MAX_LEVELS; i++)
    {
        quote eventBid = eventBids[i];
        quoteNode *insertAt;
        if (eventBid.quantity == 0)
        {
            // remove from the linked list.
            quoteNode *ptr = OBBids;
            quoteNode *prev = ptr;
            while (ptr != NULL)
            {
                if (ptr->q.price == eventBid.price)
                {
                    prev->next = ptr->next;
                    break;
                }
                prev = ptr;
                ptr = ptr->next;
            }
        }
        else
        {
            quoteNode *ptr = OBBids;
            quoteNode *prev = ptr;
            while (ptr != NULL)
            {
                if (ptr->q.price == eventBid.price)
                {
                    insertAt = NULL;
                    ptr->q.quantity = eventBid.quantity;
                    break;
                }
                else if (eventBid.price < ptr->q.price)
                {
                    insertAt = prev;
                    prev->next = ptr->next;
                    ptr->next = NULL;
                    break;
                }

                prev = ptr;
                ptr = ptr->next;
            }

            if (insertAt != NULL)
            {
                quoteNode node = {};
                node.q = eventBid;
                node.next = prev->next;
                prev->next = &node;
            }
        }
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
                LoadMarketEvent((char *)in, &marketEvent);
                if (!globalAreEventsApplied)
                {
                    BufferEvent(marketEvent, &globalMarketEventsBuffer);
                }
                else
                {
                    ApplyEvent(marketEvent, &globalOrderBook);
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
SetOrderBook(Order_book *globalOrderBook, Snapshot *globalSnapshot)
{
    char *input = (char *)globalSnapshot->resp;
    yyjson_doc *doc = yyjson_read(input, StringLength(input), 0);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *id = yyjson_obj_get(root, "lastUpdateId");
    uint64 lastUpdateId = (uint64)yyjson_get_int(id);
    printf("Last update id is %lu\n", lastUpdateId);
    globalOrderBook->lastUpdateId = lastUpdateId;

    yyjson_val *bids = yyjson_obj_get(root, "bids");
    yyjson_val *asks = yyjson_obj_get(root, "asks");
    AddLevelsToOB(asks, globalOrderBook->asks);
    AddLevelsToOB(bids, globalOrderBook->bids);
}

void PrintOrderBook(Order_book *globalOrderBook)
{
    printf("BIDS===================\n");
    quoteNode *ptr = globalOrderBook->asks;
    while (ptr != NULL)
    {
        printf("Price : %f, Quantity: %f\n", ptr->q.price, ptr->q.quantity);
        ptr = ptr->next;
    }

    printf("ASKS===================\n");
    ptr = globalOrderBook->bids;
    while (ptr != NULL)
    {
        printf("Price : %f, Quantity: %f\n", ptr->q.price, ptr->q.quantity);
        ptr = ptr->next;
    }
}

void
IgnoreAndApplyEvents(Order_book *globalOrderBook,
                     Market_events_buffer *globalMarketEventsBuffer,
                     bool *isSnapshot,
                     bool *globalAreEventsApplied)
{
    uint64 lastUpdateId = globalOrderBook->lastUpdateId;
    bool applied = false;
    for (int i = 0; i < MAX_EVENTS; i++)
    {
        Market_event event = globalMarketEventsBuffer->buffer[i];
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
            ApplyEvent(event, globalOrderBook);
            lastUpdateId = lastId;
            applied = true;
        }
        else
        {
            // Missed some events, rework the entire snapshot.
            printf("Rework the snapshot\n");
        }
    }

    if (applied)
    {
       *globalAreEventsApplied = true;
    }
    else
    {
        *isSnapshot = false;
        *globalAreEventsApplied = false;
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

    bool isSnapshot = false;
    globalMarketEventsBuffer.size = MAX_EVENTS;
    globalMarketEventsBuffer.currentWriteIndex = 0;
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
    protocol.name = "binance";
    protocol.callback = CallbackBinance;
    protocol.per_session_data_size = 256;

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
    struct lws *lws = lws_client_connect_via_info(&ccinfo);
    if (lws == NULL)
    {
        printf("Connection failed\n");
        return -1;
    }
   
    while(1)
    {
        if (globalMarketEventsBuffer.currentWriteIndex > 0 &&
            !isSnapshot) {
            printf("Checking for snapshot...\n");
            curl_easy_setopt(curl, CURLOPT_URL, SNAPSHOT_URL);
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&globalSnapshot);
            CURLcode result = curl_easy_perform(curl);
            if (result != CURLE_OK) {
                printf("curl call failed!, %s abort!\n", curl_easy_strerror(result));
            }
            char *input = (char *)globalSnapshot.resp;
            yyjson_doc *doc = yyjson_read(input, StringLength(input), 0);
            yyjson_val *root = yyjson_doc_get_root(doc);
            yyjson_val *id = yyjson_obj_get(root, "lastUpdateId");
            uint64 lastUpdateId = (uint64)yyjson_get_int(id);
            printf("Last update id is %lu\n", lastUpdateId);
            Market_event firstEvent = globalMarketEventsBuffer.buffer[0];
            printf("first update id is %lu\n", firstEvent.U);
            printf("Compare: lastUpdateId %lu with first event id %lu\n",
                   lastUpdateId, firstEvent.U);
            printf("current Write inDex is %u\n",
                   globalMarketEventsBuffer.currentWriteIndex);
            if (lastUpdateId > firstEvent.U) {
                printf("LastUpdateId %lu > the first buffered event id!", lastUpdateId);
                isSnapshot = true;
            }
        }
        else if (isSnapshot &&
                 !globalAreEventsApplied)
        {
            SetOrderBook(&globalOrderBook, &globalSnapshot);
            printf("Order book id is %lu\n", globalOrderBook.lastUpdateId);
            // discard/ignore the buffered events where the id < snapshot id
            // apply the buffered events to the order book
            IgnoreAndApplyEvents(&globalOrderBook, &globalMarketEventsBuffer,
                                 &isSnapshot, &globalAreEventsApplied);
        }

        // apply the event to the order book in the callback, if the OB is ready.
        lws_service(context, 0);
        PrintOrderBook(&globalOrderBook);
    }


    lws_context_destroy(context);
    return 0;
}
