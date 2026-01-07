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

class DBRecorder;

// Configuration constants
extern const char* RECORD_DIR;
extern const char* CONFIG_DB_JSON_PATH;

// Helper functions for database operations
int getDbIdFromName(const std::string& dbName);
std::string getDbSeparator(const std::string& dbName);

// Global state for signal handling
extern std::atomic<bool> g_stop;
extern std::atomic<unsigned int> g_rotateGen;

// Global recorder management
extern std::unordered_map<std::string, std::unique_ptr<DBRecorder>> g_recorders;
extern std::mutex g_mtx;

// Utility functions
std::string ts();
void ensureRecordDir();
std::string getLogFileName(const std::string& dbName);
std::unique_ptr<DBConnector> makeDbConnectorWithRetry(const std::string& dbName, unsigned int timeout_ms);
bool waitForPath(const std::string& path, unsigned int timeoutSeconds, bool isDirectory);
std::unordered_map<std::string, bool> read_initial_config();

// Main function components
void initializeRecorders();
void runControlLoop();
void shutdownRecorders();

class DBRecorder {
public:
    explicit DBRecorder(const std::string& dbName);
    ~DBRecorder();

    void start();
    void stop();

    int getDbId() const { return m_dbId; }
    char getSeparator() const { return m_sep; }
    bool isRunning() const { return m_running.load(); }
    const std::string& getLogPath() const { return m_logPath; }

    static std::string getOrEmpty(const std::map<std::string, std::string>& m, const char* k);
    static bool parseChannel(const std::string& ch, char sep, std::string& table, std::string& keys);

private:
    void run();
    void log_hset(const std::string& table, const std::string& key);
    void log_del(const std::string& table, const std::string& key);
    void log_hdel(const std::string& table, const std::string& key);
    void write_line(const std::string& tableKey, const char* tag, const std::string& fields);

    int m_dbId;
    std::string m_dbName;
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
