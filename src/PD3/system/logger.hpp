#pragma once

#include <iostream>
#include <string>
#include <ctime>
#include <iomanip>

#include <fmt/format.h>

namespace dpf {

enum class LogLevel {
    DEBUG,
    INFO,
    WARN,
    ERROR
};


class Logger {
public:
    static Logger& getInstance() {
        static Logger instance;
        return instance;
    }

    void setLogLevel(LogLevel level) {
        currentLevel_ = level;
    }

    // Variadic template version with source location
    template<typename... Args>
    void debug(const char* file, int line, Args&&... args) {
        if (currentLevel_ <= LogLevel::DEBUG) {
            log("DEBUG", file, line, std::forward<Args>(args)...);
        }
    }

    template<typename... Args>
    void debugf(const char* file, int line, std::string_view fmt, Args&&... args) {
        if (currentLevel_ <= LogLevel::DEBUG) {
            logf("DEBUG", file, line, fmt, std::forward<Args>(args)...);
        }
    }

    template<typename... Args>
    void info(const char* file, int line, Args&&... args) {
        if (currentLevel_ <= LogLevel::INFO) {
            log("INFO ", file, line, std::forward<Args>(args)...);
        }
    }

    template<typename... Args>
    void infof(const char* file, int line, std::string_view fmt, Args&&... args) {
        if (currentLevel_ <= LogLevel::INFO) {
            logf("INFO ", file, line, fmt, std::forward<Args>(args)...);
        }
    }

    template<typename... Args>
    void warn(const char* file, int line, Args&&... args) {
        if (currentLevel_ <= LogLevel::WARN) {
            log("WARN ", file, line, std::forward<Args>(args)...);
        }
    }

    template<typename... Args>
    void warnf(const char* file, int line, std::string_view fmt, Args&&... args) {
        if (currentLevel_ <= LogLevel::WARN) {
            logf("WARN ", file, line, fmt, std::forward<Args>(args)...);
        }
    }

    template<typename... Args>
    void error(const char* file, int line, Args&&... args) {
        if (currentLevel_ <= LogLevel::ERROR) {
            log("ERROR", file, line, std::forward<Args>(args)...);
        }
    }

    template<typename... Args>
    void errorf(const char* file, int line, std::string_view fmt, Args&&... args) {
        if (currentLevel_ <= LogLevel::ERROR) {
            logf("ERROR", file, line, fmt, std::forward<Args>(args)...);
        }
    }

private:
    Logger() : currentLevel_(LogLevel::INFO) {}
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

    template<typename T>
    void printArg(T&& arg) {
        std::cout << std::forward<T>(arg);
    }

    template<typename... Args>
    void log(const char* level, const char* file, int line, Args&&... args) {
        writeHeader(level, file, line);
        (printArg(std::forward<Args>(args)), ...);
        std::cout << std::endl;
    }

    template<typename... Args>
    void logf(const char* level, const char* file, int line, std::string_view fmt, Args&&... args) {
        writeHeader(level, file, line);
        #if defined(__cpp_lib_format)
            std::cout << std::format(fmt, std::forward<Args>(args)...);
        #else
            std::cout << fmt::format(fmt, std::forward<Args>(args)...);
        #endif
        std::cout << std::endl;
    }

    void writeHeader(const char* level, const char* file, int line) {
        auto now = std::time(nullptr);
        auto* tm = std::localtime(&now);
        const char* filename = strrchr(file, '/');  // Find last occurrence of '/'
        filename = filename ? filename + 1 : file;   // If found, move past '/', otherwise use full string
        std::cout << "[" << std::put_time(tm, "%Y-%m-%d %H:%M:%S") << "] "
                  << "[" << level << "] "
                  << "[" << filename << ":" << line << "] ";
    }

    LogLevel currentLevel_;
};

// Updated convenience macros for getting logger instance with file and line info
#define LOG_DEBUG(...) Logger::getInstance().debug(__FILE__, __LINE__, __VA_ARGS__)
#define LOG_DEBUGF(...) Logger::getInstance().debugf(__FILE__, __LINE__, __VA_ARGS__)
#define LOG_INFO(...) Logger::getInstance().info(__FILE__, __LINE__, __VA_ARGS__)
#define LOG_INFOF(...) Logger::getInstance().infof(__FILE__, __LINE__, __VA_ARGS__)
#define LOG_WARN(...) Logger::getInstance().warn(__FILE__, __LINE__, __VA_ARGS__)
#define LOG_WARNF(...) Logger::getInstance().warnf(__FILE__, __LINE__, __VA_ARGS__)
#define LOG_ERROR(...) Logger::getInstance().error(__FILE__, __LINE__, __VA_ARGS__)
#define LOG_ERRORF(...) Logger::getInstance().errorf(__FILE__, __LINE__, __VA_ARGS__)

} // namespace dpf