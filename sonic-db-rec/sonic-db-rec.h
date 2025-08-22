#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <fstream>
#include <map>

#include "common/dbconnector.h"
#include "common/dbinterface.h"
#include "common/pubsub.h"

using swss::DBConnector;
using swss::PubSub;

// Constants for testing
extern const std::unordered_map<std::string, int> DB_MAP;
extern const std::unordered_map<int, std::string> LOG_NAME;

// Global state for signal handling
extern std::atomic<bool> g_stop;
extern std::atomic<unsigned int> g_rotateGen;

// Utility functions
std::string ts();
void ensureRecordDir();
std::unique_ptr<DBConnector> makeDbConnectorWithRetry(int dbId, const std::string &host, int port, unsigned int timeout_ms);
std::unordered_map<std::string, bool> read_initial_config();

class DBRecorder {
public:
    explicit DBRecorder(int db);
    ~DBRecorder();

    void start();
    void stop();

    // Public interface for testing
    int getDbId() const { return m_dbId; }
    char getSeparator() const { return m_sep; }
    bool isRunning() const { return m_running.load(); }
    const std::string& getLogPath() const { return m_logPath; }

    // Static utility methods for testing
    static std::string getOrEmpty(const std::map<std::string, std::string>& m, const char* k);
    static bool parseChannel(const std::string& ch, char sep, std::string& table, std::string& keys);

private:
    void run();
    void log_hset(const std::string& table, const std::string& key);
    void log_del(const std::string& table, const std::string& key);
    void log_hdel(const std::string& table, const std::string& key);
    void write_line(const std::string& tableKey, const char* tag, const std::string& fields);

    int m_dbId;
    char m_sep;
    std::unique_ptr<DBConnector> m_conn;
    std::unique_ptr<PubSub> m_pubsub;
    std::unique_ptr<std::thread> m_thr;
    std::atomic<bool> m_running{false};
    std::ofstream m_log;
    std::string m_logPath;
    unsigned int m_seenRotateGen{0};
};

// Signal handlers
void handle_signal(int sig);
void handle_sighup(int sig);
