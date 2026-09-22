#ifndef LOG_H
#define LOG_H

typedef enum
{
    LOG_ERROR = 0,
    LOG_WARN,
    LOG_INFO,
    LOG_DEBUG
} LogLevel;

void LogInit(LogLevel level);
void LogSetLevel(LogLevel level);

void LogError(const char *fmt, ...);
void LogWarn(const char *fmt, ...);
void LogInfo(const char *fmt, ...);
void LogDebug(const char *fmt, ...);

#endif
