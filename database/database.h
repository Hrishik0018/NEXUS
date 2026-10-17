// PostgreSQL data-access layer (libpq). One connection guarded by a mutex:
// simple, thread-safe, and plenty for a prototype. Every query is
// parameterised ($1, $2 ...) so no SQL is built from network input.
#pragma once

#include <mutex>
#include <string>
#include <vector>

typedef struct pg_conn PGconn;

namespace nexus {

class Database {
public:
    struct Config {
        std::string host, port, name, user, password;
        static Config fromEnv();   // NEXUS_DB_HOST / _PORT / _NAME / _USER / _PASSWORD
    };

    struct Result {
        bool ok = false;
        std::string error;
        std::vector<std::string> columns;
        std::vector<std::vector<std::string>> rows;
    };

    explicit Database(Config cfg) : cfg_(std::move(cfg)) {}
    ~Database();
    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

    bool connect(std::string& err);

    // Generic parameterised query (used by the CLI table views too).
    Result exec(const std::string& sql, const std::vector<std::string>& params = {});

    // ---- Workers -----------------------------------------------------
    bool markAllWorkersInactive();
    bool upsertWorker(const std::string& id, const std::string& hostname,
                      const std::string& ip, int cpuCores, long long memMb);
    bool recordHeartbeat(const std::string& id);
    // Updates live state in `workers` AND appends a row to `resource_metrics`.
    bool recordResources(const std::string& id, double cpu, double ram, double disk);
    bool setWorkerStatus(const std::string& id, const std::string& status);

    // ---- Jobs / tasks ------------------------------------------------
    bool createJobAndTask(const std::string& type, const std::string& params,
                          long long& jobId, long long& taskId, std::string& err);
    bool markTaskAssigned(long long taskId, const std::string& workerId);
    bool markTaskStarted(long long taskId);
    bool markTaskCompleted(long long taskId, const std::string& result);
    bool markTaskFailed(long long taskId, const std::string& reason);
    bool recordExecution(long long taskId, const std::string& workerId,
                         const std::string& taskType, double execSeconds,
                         const std::string& status, double cpuAtAssign,
                         double ramAtAssign, const std::string& policy);

private:
    bool ensureConnected(std::string& err);   // caller holds mutex_

    Config cfg_;
    PGconn* conn_ = nullptr;
    std::mutex mutex_;
};

}  // namespace nexus
