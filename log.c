#include "log.h"

#include <stdio.h>
#include <stdarg.h>
#include <time.h>

static LogLevel currentLevel = LOG_INFO;

void
LogInit(LogLevel level)
{
    currentLevel = level;
}

void
LogSetLevel(LogLevel level)
{
    currentLevel = level;
}

static void
LogMessage(LogLevel level, const char *name, const char *fmt, va_list args)
{
    if (level > currentLevel)
        return;

    time_t now = time(NULL);
    struct tm tm_now;

    localtime_r(&now, &tm_now);

    char timestamp[32];

    strftime(
        timestamp,
        sizeof(timestamp),
        "%Y-%m-%d %H:%M:%S",
        &tm_now
    );

    fprintf(
        stderr,
        "[%s] %-5s ",
        timestamp,
        name
    );

    vfprintf(stderr, fmt, args);

    fprintf(stderr, "\n");
}

void
LogError(const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);

    LogMessage(LOG_ERROR, "ERROR", fmt, args);

    va_end(args);
}

void
LogWarn(const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);

    LogMessage(LOG_WARN, "WARN", fmt, args);

    va_end(args);
}

void
LogInfo(const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);

    LogMessage(LOG_INFO, "INFO", fmt, args);

    va_end(args);
}

void
LogDebug(const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);

    LogMessage(LOG_DEBUG, "DEBUG", fmt, args);

    va_end(args);
}
