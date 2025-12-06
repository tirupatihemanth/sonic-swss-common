#include <atomic>
#include <chrono>
#include <csignal>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/inotify.h>
#include <unistd.h>
#include <errno.h>
#include <cstring>

#include "common/dbconnector.h"
#include "common/dbinterface.h"
#include "common/redisreply.h"
#include "common/pubsub.h"
#include "common/logger.h"
#include <nlohmann/json.hpp>
#include "sonic-db-rec.h"

using namespace std::chrono_literals;
using swss::DBConnector;
using swss::PubSub;
using swss::SonicDBConfig;

std::atomic<bool> g_stop{false};
std::atomic<unsigned int> g_rotateGen{0};

// Global recorder management
std::unordered_map<std::string, std::unique_ptr<DBRecorder>> g_recorders;
std::mutex g_mtx;

// Configuration constants
const char* RECORD_DIR = "/var/log/record";
const char* CONFIG_DB_JSON_PATH = "/etc/sonic/config_db.json";
const char* DATABASE_CONFIG_JSON_PATH = "/var/run/redis/sonic-db/database_config.json";
const unsigned int DATABASE_CONFIG_JSON_TIMEOUT = 600; // Seconds to wait for database_config.json to become available at boot

// Helper functions for database name or id
int getDbIdFromName(const std::string& dbName) {
    try {
        return SonicDBConfig::getDbId(dbName);
    } catch (const std::exception& e) {
        SWSS_LOG_ERROR("Failed to get DB ID for database %s: %s", dbName.c_str(), e.what());
        return -1;
    }
}

std::string getDbSeparator(const std::string& dbName) {
    try {
        return SonicDBConfig::getSeparator(dbName);
    } catch (const std::exception& e) {
        SWSS_LOG_ERROR("Failed to get separator for database %s: %s", dbName.c_str(), e.what());
        return ":";  // default separator
    }
}

std::string ts()
{
    // 2025-01-01.12:34:56.123456
    auto now = std::chrono::system_clock::now();
    auto tt  = std::chrono::system_clock::to_time_t(now);
    auto us  = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()) % 1000000;

    std::tm tm{};
    localtime_r(&tt, &tm);

    char buf[64];
    std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d.%02d:%02d:%02d.%06ld",
                  tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                  tm.tm_hour, tm.tm_min, tm.tm_sec, static_cast<long>(us.count()));
    return std::string(buf);
}

void ensureRecordDir()
{
    struct stat st;
    if (stat(RECORD_DIR, &st) == 0)
    {
        if (S_ISDIR(st.st_mode))
        {
            return;
        }
    }

    if (mkdir(RECORD_DIR, 0755) != 0 && errno != EEXIST)
    {
        SWSS_LOG_WARN("failed to create %s: %s", RECORD_DIR, std::strerror(errno));
    }
}

std::string getLogFileName(const std::string& dbName)
{
    std::string lowercaseDbName = dbName;
    std::transform(lowercaseDbName.begin(), lowercaseDbName.end(), lowercaseDbName.begin(), [](unsigned char uc){ return static_cast<char>(std::tolower(uc)); });
    return std::string(RECORD_DIR) + "/" + lowercaseDbName + ".rec";
}

std::unique_ptr<DBConnector> makeDbConnectorWithRetry(const std::string& dbName, unsigned int timeout_ms)
{
    using namespace std::chrono_literals;
    while (!g_stop.load()) {
        try {
            std::unique_ptr<DBConnector> conn(new DBConnector(dbName, timeout_ms, false));
            return conn;
        } catch (const std::exception &e) {
            SWSS_LOG_WARN("Waiting for Redis db %s: %s", dbName.c_str(), e.what());
            std::this_thread::sleep_for(500ms);
        }
    }
    return nullptr;
}

// DBRecorder method implementations

DBRecorder::DBRecorder(const std::string& dbName)
    : m_dbId(getDbIdFromName(dbName)),
      m_dbName(dbName),
      m_sep(getDbSeparator(dbName)[0]),  // Take first character of separator string
      m_conn(makeDbConnectorWithRetry(dbName, 0)),
      m_pubsub(m_conn ? new PubSub(m_conn.get()) : nullptr)
{
    if (m_dbId < 0) {
        throw std::runtime_error("Invalid database name: " + dbName);
    }
    if (!m_conn) {
        throw std::runtime_error("DBRecorder constructed without Redis connection");
    }
    m_logPath = getLogFileName(dbName);
    m_log.open(m_logPath, std::ios::out | std::ios::app);
    if (!m_log.is_open()) {
        SWSS_LOG_WARN("Failed to open log file %s", m_logPath.c_str());
    }
    m_seenRotateGen = g_rotateGen.load();
}

DBRecorder::~DBRecorder() {
    stop();
}

void DBRecorder::start() {
    if (m_running.exchange(true)) return;
    const std::string pattern = "__keyspace@" + std::to_string(m_dbId) + "__:*";
    m_pubsub->psubscribe(pattern);
    m_thr.reset(new std::thread(&DBRecorder::run, this));
    SWSS_LOG_INFO("Recorder started for db %d", m_dbId);
}

void DBRecorder::stop() {
    if (!m_running.exchange(false)) return;
    try {
        if (m_pubsub) {
            // best-effort unsubscribe: pattern must match what we subscribed
            const std::string pattern = "__keyspace@" + std::to_string(m_dbId) + "__:*";
            m_pubsub->punsubscribe(pattern);
        }
    } catch (...) {}
    if (m_thr && m_thr->joinable()) {
        m_thr->join();
    }
    if (m_log.is_open()) m_log.flush();
    SWSS_LOG_INFO("Recorder stopped for db %d", m_dbId);
}

void DBRecorder::run() {
    while (m_running && !g_stop) {
        std::map<std::string, std::string> msg;
        try {
            msg = m_pubsub->get_message(0.5 /*sec*/, true);
        } catch (...) {
            std::this_thread::sleep_for(500ms);
            continue;
        }
        if (msg.empty()) continue;

        const auto itType = msg.find("type");
        if (itType == msg.end() || (itType->second != "pmessage" && itType->second != "message")) continue;

        const std::string ch   = getOrEmpty(msg, "channel");
        const std::string op   = getOrEmpty(msg, "data");

        if (ch.empty() || op.empty()) continue;

        std::string table, keys;
        if (!parseChannel(ch, m_sep, table, keys)) continue;


        if (op == "del") {
            log_del(table, keys);
        } else if (op == "hset") {
            log_hset(table, keys);
        } else if (op == "hdel") {
            log_hdel(table, keys);
        } else {
            // Fallback: log unhandled operation
            SWSS_LOG_INFO("Unhandled op %s table %s key %s (db %d)", op.c_str(), table.c_str(), keys.c_str(), m_dbId);
        }
    }
}

std::string DBRecorder::getOrEmpty(const std::map<std::string, std::string>& m, const char* k) {
    auto it = m.find(k);
    return it == m.end() ? std::string() : it->second;
}

bool DBRecorder::parseChannel(const std::string& ch, char sep, std::string& table, std::string& keys) {
    // "__keyspace@<db>__:<table><sep><keys>"
    auto posColon = ch.find(':');
    if (posColon == std::string::npos) return false;
    auto posSep = ch.find(sep, posColon + 1);
    if (posSep == std::string::npos || posSep <= posColon + 1) return false;

    table = ch.substr(posColon + 1, posSep - (posColon + 1));
    keys  = ch.substr(posSep + 1);
    return true;
}

void DBRecorder::log_hset(const std::string& table, const std::string& key) {
    const std::string redisKey = table + m_sep + key;
    auto h = m_conn->hgetall<std::unordered_map<std::string, std::string>>(redisKey);
    const std::string tableKey = table + ":" + key;
    std::string fields;
    for (const auto& kv : h) {
        if (!fields.empty()) fields += "|";
        fields += kv.first + ":" + kv.second;
    }
    write_line(tableKey, "SET", fields);
}

void DBRecorder::log_del(const std::string& table, const std::string& key) {
    const std::string tableKey = table + ":" + key;
    write_line(tableKey, "DEL", "");
}

void DBRecorder::log_hdel(const std::string& table, const std::string& key) {
    const std::string full_key = table.empty() ? key : (table + m_sep + key);
    auto h = m_conn->hgetall<std::unordered_map<std::string, std::string>>(full_key);
    const std::string tableKey = table + ":" + key;
    if (!h.empty()) {
        std::string fields;
        for (const auto& kv : h) {
            if (!fields.empty()) fields += "|";
            fields += kv.first + ":" + kv.second;
        }
        write_line(tableKey, "HDEL", fields);
    } else {
        write_line(tableKey, "HDEL", "");
    }
}

void DBRecorder::write_line(const std::string& tableKey, const char* tag, const std::string& fields) {
    // Handle logrotate: reopen file if SIGHUP occurred
    unsigned int gen = g_rotateGen.load();
    if (gen != m_seenRotateGen) {
        if (m_log.is_open()) m_log.close();
        m_log.open(m_logPath, std::ofstream::out | std::ofstream::app);
        if (!m_log.is_open()) {
            SWSS_LOG_ERROR("Failed to reopen log file %s after SIGHUP", m_logPath.c_str());
        } else {
            SWSS_LOG_INFO("Reopened log file %s after SIGHUP", m_logPath.c_str());
        }
        m_seenRotateGen = gen;
    }
    std::string line = ts();
    line += "|";
    line += tableKey;
    line += "|";
    line += tag;
    if (!fields.empty()) {
        line += "|";
        line += fields;
    }
    line += "\n";
    if (m_log.is_open()) {
        m_log << line;
        m_log.flush();
    } else {
        // fallback
        SWSS_LOG_INFO("%s", line.c_str());
    }
}

bool waitForPath(const std::string& path, unsigned int timeoutSeconds, bool isDirectory)
{
    // First check if path already exists
    struct stat st;
    if (stat(path.c_str(), &st) == 0) {
        if (isDirectory) {
            return S_ISDIR(st.st_mode);
        }
        // For files, ensure they have content (not empty/being written)
        if (st.st_size > 0) {
            return true;  // File exists and has content
        }
        // File exists but is empty, fall through to wait for content
        SWSS_LOG_INFO("File %s exists but is empty, waiting for content...", path.c_str());
    }

    // Extract parent directory and name
    std::string parentDir;
    std::string name;
    size_t pos = path.find_last_of('/');
    if (pos != std::string::npos) {
        if (pos > 0) {
            parentDir = path.substr(0, pos);
            name = path.substr(pos + 1);
        } else {
            parentDir = "/";
            name = path.substr(pos + 1);
        }
    } else {
        parentDir = ".";
        name = path;
    }

    // Recursively wait for parent directory if it doesn't exist
    if (stat(parentDir.c_str(), &st) != 0 || !S_ISDIR(st.st_mode)) {
        SWSS_LOG_INFO("Parent directory %s does not exist, waiting for it first...", parentDir.c_str());
        if (!waitForPath(parentDir, timeoutSeconds, true)) {
            SWSS_LOG_ERROR("Parent directory %s did not become available", parentDir.c_str());
            return false;
        }
    }

    int inotifyFd = inotify_init();
    if (inotifyFd < 0) {
        SWSS_LOG_ERROR("inotify_init failed: %s", std::strerror(errno));
        return false;
    }

    // For directories, watch CREATE and MOVED_TO
    // For files, only watch CLOSE_WRITE (guarantees file is fully written)
    uint32_t mask = isDirectory ? (IN_CREATE | IN_MOVED_TO) : IN_CLOSE_WRITE;
    int watchFd = inotify_add_watch(inotifyFd, parentDir.c_str(), mask);
    if (watchFd < 0) {
        SWSS_LOG_ERROR("inotify_add_watch failed for %s: %s", parentDir.c_str(), std::strerror(errno));
        close(inotifyFd);
        return false;
    }

    bool found = false;
    auto startTime = std::chrono::steady_clock::now();
    const auto timeout = std::chrono::seconds(timeoutSeconds);

    while (!found && !g_stop.load()) {
        auto elapsed = std::chrono::steady_clock::now() - startTime;
        if (elapsed >= timeout) {
            SWSS_LOG_WARN("Timeout waiting for %s %s after %u seconds", 
                         isDirectory ? "directory" : "file", path.c_str(), timeoutSeconds);
            break;
        }

        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(inotifyFd, &readfds);
        
        struct timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 0;

        int ret = select(inotifyFd + 1, &readfds, nullptr, nullptr, &tv);
        if (ret < 0) {
            if (errno == EINTR) continue;
            SWSS_LOG_ERROR("select failed: %s", std::strerror(errno));
            break;
        }

        if (ret == 0) continue;

        if (FD_ISSET(inotifyFd, &readfds)) {
            char buffer[4096] __attribute__ ((aligned(__alignof__(struct inotify_event))));
            ssize_t len = read(inotifyFd, buffer, sizeof(buffer));
            if (len < 0) {
                SWSS_LOG_ERROR("read from inotify failed: %s", std::strerror(errno));
                break;
            }

            const struct inotify_event* event;
            for (char* ptr = buffer; ptr < buffer + len; ptr += sizeof(struct inotify_event) + event->len) {
                event = reinterpret_cast<const struct inotify_event*>(ptr);
                if (event->len > 0 && name == event->name) {
                    // Verify the path exists
                    if (stat(path.c_str(), &st) == 0) {
                        if (isDirectory && !S_ISDIR(st.st_mode)) {
                            continue;  // Not a directory, keep waiting
                        }
                        if (!isDirectory && st.st_size == 0) {
                            SWSS_LOG_WARN("File %s closed but is empty", path.c_str());
                            continue;  // Empty file, keep waiting
                        }
                        found = true;
                        SWSS_LOG_INFO("%s %s is now available%s", 
                                     isDirectory ? "Directory" : "File", 
                                     path.c_str(),
                                     isDirectory ? "" : " (fully written)");
                        break;
                    }
                }
            }
        }
    }

    inotify_rm_watch(inotifyFd, watchFd);
    close(inotifyFd);
    return found;
}

std::unordered_map<std::string, bool> read_initial_config()
{
    std::unordered_map<std::string, bool> result;
    try {
        std::ifstream f(CONFIG_DB_JSON_PATH);
        if (!f.is_open()) {
            SWSS_LOG_ERROR("Failed to open %s", CONFIG_DB_JSON_PATH);
            return result;
        }
        nlohmann::json j;
        f >> j;
        if (!j.contains("RECORDER") || !j["RECORDER"].is_object()) {
            return result;
        }

        for (auto it = j["RECORDER"].begin(); it != j["RECORDER"].end(); ++it) {
            const std::string name = it.key();
            const auto& val = it.value();
            bool enabled = false;
            if (val.is_object()) {
                auto sit = val.find("state");
                if (sit != val.end() && sit->is_string()) {
                    std::string s = sit->get<std::string>();
                    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char uc){ return static_cast<char>(std::tolower(uc)); });
                    enabled = (s == "enabled");
                }
            }
            result[name] = enabled;
        }
    } catch (const std::exception& e) {
        SWSS_LOG_ERROR("read_initial_config exception: %s", e.what());
    }
    return result;
}

void handle_signal(int) { g_stop = true; }
void handle_sighup(int) { g_rotateGen.fetch_add(1, std::memory_order_relaxed); }

void initializeRecorders()
{
    // Initial config
    auto init = read_initial_config();
    for (const auto& kv : init) {
        // Normalize database name to uppercase for consistent lookup
        std::string dbName = kv.first;
        std::transform(dbName.begin(), dbName.end(), dbName.begin(),
                      [](unsigned char c){ return static_cast<char>(std::toupper(c)); });

        int dbId = getDbIdFromName(dbName);
        if (kv.second && dbId >= 0) {
            auto rec = std::unique_ptr<DBRecorder>(new DBRecorder(dbName));
            rec->start();
            g_recorders.emplace(dbName, std::move(rec));
            SWSS_LOG_NOTICE("started recorder for %s (db %d)", dbName.c_str(), dbId);
        }
    }
}

void runControlLoop()
{
    // Control loop on CONFIG_DB: "__keyspace@<config_db_id>__:RECORDER*"
    std::unique_ptr<DBConnector> conf = makeDbConnectorWithRetry("CONFIG_DB", 0);
    if (!conf) {
        SWSS_LOG_ERROR("Exiting: could not connect to CONFIG_DB");
        return;
    }
    int configDbId = getDbIdFromName("CONFIG_DB");
    std::string configKeyspacePattern = "__keyspace@" + std::to_string(configDbId) + "__:RECORDER*";
    std::unique_ptr<PubSub> cps(new PubSub(conf.get()));
    cps->psubscribe(configKeyspacePattern);

    while (!g_stop) {
        std::map<std::string, std::string> msg;
        try {
            msg = cps->get_message(0.5 /*sec*/, true);
        } catch (...) {
            std::this_thread::sleep_for(500ms);
            continue;
        }
        if (msg.empty()) continue;

        const auto type = msg.count("type") ? msg["type"] : "";
        if (type != "pmessage" && type != "message") continue;

        const std::string ch = msg.count("channel") ? msg.at("channel") : "";
        const std::string op = msg.count("data") ? msg.at("data") : "";
        if (ch.empty() || op.empty()) continue;


        if (op != "hset" && op != "hdel" && op != "del") continue;

        // ch like "__keyspace@4__:RECORDER|CONFIG_DB"
        auto pos = ch.find(':');
        if (pos == std::string::npos) continue;
        std::string key = ch.substr(pos + 1);
        if (key.find("RECORDER|") != 0) continue;
        std::string name = key.substr(std::string("RECORDER|").size());

        // Normalize to uppercase for consistent lookup
        std::transform(name.begin(), name.end(), name.begin(), 
                      [](unsigned char c){ return static_cast<char>(std::toupper(c)); });

        int dbId = getDbIdFromName(name);
        if (dbId < 0) continue;

        // Read state
        std::string redisKey = "RECORDER|" + name;
        auto h = conf->hgetall<std::unordered_map<std::string, std::string>>(redisKey);
        std::string state = "disabled";
        auto sit = h.find("state");
        if (sit != h.end()) {
            state = sit->second;
            std::transform(state.begin(), state.end(), state.begin(), [](unsigned char uc){ return static_cast<char>(std::tolower(uc)); });
        }
        bool want_enabled = (state == "enabled");


        std::lock_guard<std::mutex> lk(g_mtx);
        bool have = (g_recorders.find(name) != g_recorders.end());
        if (want_enabled && !have) {
            auto rec = std::unique_ptr<DBRecorder>(new DBRecorder(name));
            rec->start();
            g_recorders.emplace(name, std::move(rec));
            SWSS_LOG_NOTICE("enabled recorder for %s (db %d)", name.c_str(), dbId);
        } else if (!want_enabled && have) {
            g_recorders[name]->stop();
            g_recorders.erase(name);
            SWSS_LOG_NOTICE("disabled recorder for %s", name.c_str());
        }
    }

    // Cleanup pubsub
    try { cps->punsubscribe(configKeyspacePattern); } catch (...) {}
}

void shutdownRecorders()
{
    std::lock_guard<std::mutex> lk(g_mtx);
    for (auto &kv : g_recorders) {
        kv.second->stop();
    }
    g_recorders.clear();
}

int main()
{
    if (std::signal(SIGINT, handle_signal) == SIG_ERR)
    {
        SWSS_LOG_ERROR("failed to setup SIGINT action");
        exit(1);
    }

    if (std::signal(SIGTERM, handle_signal) == SIG_ERR)
    {
        SWSS_LOG_ERROR("failed to setup SIGTERM action");
        exit(1);
    }

    if (std::signal(SIGHUP, handle_sighup) == SIG_ERR)
    {
        SWSS_LOG_ERROR("failed to setup SIGHUP action");
        exit(1);
    }

    ensureRecordDir();

    // Ensure database_config.json exists before initializing SonicDBConfig
    // This will automatically wait for parent directories if they don't exist
    if (!waitForPath(DATABASE_CONFIG_JSON_PATH, DATABASE_CONFIG_JSON_TIMEOUT, false))
    {
        SWSS_LOG_ERROR("Database config file %s did not become available within timeout", DATABASE_CONFIG_JSON_PATH);
        exit(1);
    }

    // Initialize SonicDBConfig to read database configuration
    try {
        SonicDBConfig::initialize();
        SWSS_LOG_INFO("SonicDBConfig initialized successfully");
    } catch (const std::exception& e) {
        SWSS_LOG_ERROR("Failed to initialize SonicDBConfig: %s", e.what());
        exit(1);
    }

    SWSS_LOG_INFO("Starting sonic-db-rec (C++14)");

    initializeRecorders();
    runControlLoop();
    shutdownRecorders();

    SWSS_LOG_INFO("Exiting sonic-db-rec (C++14)");
    return 0;
}