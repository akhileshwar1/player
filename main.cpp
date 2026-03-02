#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <libwebsockets.h>
#include <yyjson.h>

#define MARKET_BASE_ENDP "stream.binance.com"
#define STREAM_PATH "/ws/bnbbtc@depth"
#define MAX_LEVELS 10
#define MAX_EVENTS 10 

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
    Market_event *buffer[MAX_EVENTS];
} Market_events_buffer;

Market_events_buffer globalMarketEventsBuffer = {};

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
AddLevelsToEvent(yyjson_val *val, quote quotes[])
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
                q.quantity = f;
            }
            else
            {
                q.price = f;
            }
            printf("q %d, %f\n", (int)j, f);
        }
        quotes[idx] = q;
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
    AddLevelsToEvent(a, event->asks);
    AddLevelsToEvent(b, event->bids);
    yyjson_doc_free(doc);
}

void
BufferEvent(Market_event *marketEvent, Market_events_buffer *marketEventsBuffer)
{
    uint16 currentWriteIndex = marketEventsBuffer->currentWriteIndex;
    uint16 size = marketEventsBuffer->size;
    if (currentWriteIndex >= size)
    {
        marketEventsBuffer->currentWriteIndex = currentWriteIndex % size;
    }
    marketEventsBuffer->buffer[marketEventsBuffer->currentWriteIndex] = marketEvent;
    printf("Buffered event U %u at index %u\n",
           marketEvent->U,
           marketEventsBuffer->currentWriteIndex);
    marketEventsBuffer->currentWriteIndex++;
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
                LoadMarketEvent((char *)in, &marketEvent);
                BufferEvent(&marketEvent, &globalMarketEventsBuffer);
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

int
main()
{
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
        lws_service(context, 0);
    }

    lws_context_destroy(context);
    return 0;
}
