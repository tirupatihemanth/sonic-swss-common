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
#include <errno.h>
#include <cstring>

#include "common/dbconnector.h"
#include "common/dbinterface.h"
#include "common/redisreply.h"
#include "common/pubsub.h"
#include "common/logger.h"
#include <nlohmann/json.hpp>

using namespace std::chrono_literals;
using swss::DBConnector;
using swss::PubSub;

namespace {

static const std::unordered_map<std::string, int> DB_MAP = {
    {"config_db", 4},
    {"state_db", 6},
};

static const std::unordered_map<int, std::string> LOG_NAME = {
    {4, "config"},
    {6, "state"},
};

static std::atomic<bool> g_stop{false};
static std::atomic<unsigned int> g_rotateGen{0};

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

static void ensureRecordDir()
{
    const char* dir = "/var/log/record";
    struct stat st;
    if (stat(dir, &st) == 0)
    {
        if (S_ISDIR(st.st_mode))
        {
            return;
        }
    }

    if (mkdir(dir, 0755) != 0 && errno != EEXIST)
    {
        SWSS_LOG_WARN("failed to create %s: %s", dir, std::strerror(errno));
    }
}

static std::unique_ptr<DBConnector> makeDbConnectorWithRetry(int dbId, const std::string &host, int port, unsigned int timeout_ms)
{
    using namespace std::chrono_literals;
    while (!g_stop.load()) {
        try {
            std::unique_ptr<DBConnector> conn(new DBConnector(dbId, host, port, timeout_ms));
            return conn;
        } catch (const std::exception &e) {
            SWSS_LOG_WARN("Waiting for Redis db %d at %s:%d: %s", dbId, host.c_str(), port, e.what());
            std::this_thread::sleep_for(500ms);
        }
    }
    return nullptr;
}

class DBRecorder {
public:
    explicit DBRecorder(int db)
    : m_dbId(db),
      m_sep((db == 4 || db == 6) ? '|' : ':'),
      m_conn(makeDbConnectorWithRetry(db, "127.0.0.1", 6379, 0)),
      m_pubsub(m_conn ? new PubSub(m_conn.get()) : nullptr)
    {
        if (!m_conn) {
            throw std::runtime_error("DBRecorder constructed without Redis connection");
        }
        auto it = LOG_NAME.find(db);
        const std::string name = (it != LOG_NAME.end()) ? it->second : std::to_string(db);
        const std::string path = "/var/log/record/" + name + ".rec";
        m_logPath = path;
        m_log.open(m_logPath, std::ios::out | std::ios::app);
        if (!m_log.is_open()) {
            SWSS_LOG_WARN("Failed to open log file %s", m_logPath.c_str());
        }
        m_seenRotateGen = g_rotateGen.load();
    }

    ~DBRecorder() {
        stop();
    }

    void start() {
        if (m_running.exchange(true)) return;
        const std::string pattern = "__keyspace@" + std::to_string(m_dbId) + "__:*";
        m_pubsub->psubscribe(pattern);
        m_thr.reset(new std::thread(&DBRecorder::run, this));
        SWSS_LOG_INFO("Recorder started for db %d", m_dbId);
    }

    void stop() {
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

private:
    void run() {
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

    static std::string getOrEmpty(const std::map<std::string, std::string>& m, const char* k) {
        auto it = m.find(k);
        return it == m.end() ? std::string() : it->second;
    }

    static bool parseChannel(const std::string& ch, char sep, std::string& table, std::string& keys) {
        // "__keyspace@<db>__:<table><sep><keys>"
        auto posColon = ch.find(':');
        if (posColon == std::string::npos) return false;
        auto posSep = ch.find(sep, posColon + 1);
        if (posSep == std::string::npos || posSep <= posColon + 1) return false;

        table = ch.substr(posColon + 1, posSep - (posColon + 1));
        keys  = ch.substr(posSep + 1);
        return true;
    }

    void log_hset(const std::string& table, const std::string& key) {
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

    void log_del(const std::string& table, const std::string& key) {
        const std::string tableKey = table + ":" + key;
        write_line(tableKey, "DEL", "");
    }

    void log_hdel(const std::string& table, const std::string& key) {
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

    void write_line(const std::string& tableKey, const char* tag, const std::string& fields) {
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

private:
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

std::unordered_map<std::string, bool> read_initial_config()
{
    std::unordered_map<std::string, bool> result;
    try {
        std::ifstream f("/etc/sonic/config_db.json");
        if (!f.is_open()) {
            SWSS_LOG_ERROR("Failed to open /etc/sonic/config_db.json");
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

} // namespace

static void handle_signal(int) { g_stop = true; }
static void handle_sighup(int) { g_rotateGen.fetch_add(1, std::memory_order_relaxed); }

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

    SWSS_LOG_INFO("Starting sonic-db-rec (C++14)");

    std::unordered_map<std::string, std::unique_ptr<DBRecorder>> recorders;
    std::mutex mtx;

    // Initial config
    auto init = read_initial_config();
    for (const auto& kv : init) {
        auto it = DB_MAP.find(kv.first);
        if (kv.second && it != DB_MAP.end()) {
            auto rec = std::unique_ptr<DBRecorder>(new DBRecorder(it->second));
            rec->start();
            recorders.emplace(kv.first, std::move(rec));
            SWSS_LOG_NOTICE("started recorder for %s (db %d)", kv.first.c_str(), it->second);
        }
    }

    // Control loop on CONFIG_DB: "__keyspace@4__:RECORDER*"
    std::unique_ptr<DBConnector> conf = makeDbConnectorWithRetry(4, "127.0.0.1", 6379, 0);
    if (!conf) {
        SWSS_LOG_ERROR("Exiting: could not connect to CONFIG_DB");
        return 1;
    }
    std::unique_ptr<PubSub> cps(new PubSub(conf.get()));
    cps->psubscribe("__keyspace@4__:RECORDER*");

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

        // ch like "__keyspace@4__:RECORDER|config_db"
        auto pos = ch.find(':');
        if (pos == std::string::npos) continue;
        std::string key = ch.substr(pos + 1);
        if (key.find("RECORDER|") != 0) continue;
        std::string name = key.substr(std::string("RECORDER|").size());


        auto it = DB_MAP.find(name);
        if (it == DB_MAP.end()) continue;

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


        std::lock_guard<std::mutex> lk(mtx);
        bool have = (recorders.find(name) != recorders.end());
        if (want_enabled && !have) {
            auto rec = std::unique_ptr<DBRecorder>(new DBRecorder(it->second));
            rec->start();
            recorders.emplace(name, std::move(rec));
            SWSS_LOG_NOTICE("enabled recorder for %s (db %d)", name.c_str(), it->second);
        } else if (!want_enabled && have) {
            recorders[name]->stop();
            recorders.erase(name);
            SWSS_LOG_NOTICE("disabled recorder for %s", name.c_str());
        }
    }

    // Shutdown
    try { cps->punsubscribe("__keyspace@4__:RECORDER*"); } catch (...) {}
    {
        std::lock_guard<std::mutex> lk(mtx);
        for (auto &kv : recorders) {
            kv.second->stop();
        }
        recorders.clear();
    }

    SWSS_LOG_INFO("Exiting sonic-db-rec (C++14)");
    return 0;
}