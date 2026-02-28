#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <libwebsockets.h>

#define MARKET_BASE_ENDP "stream.binance.com"
#define STREAM_PATH "/ws/bnbbtc@depth"

typedef uint32_t uint32;

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
            ((char *)in)[len] = '\0';
            printf("rx %d '%s'\n", (int)len, (char *)in);
            break;

        default:
            break;
    }
    return 0;
}

static struct lws_protocols protocols[] = {
    {
        "binance",          // Name of the protocol
        CallbackBinance,    // Your function pointer
        0,                  // per_session_data_size (0 if not using)
        1024,               // rx_buffer_size
    },
    { NULL, NULL, 0, 0 }    // Terminator - ALWAYS REQUIRED
};

int
main()
{
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
