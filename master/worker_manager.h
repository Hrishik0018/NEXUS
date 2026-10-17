// WorkerManager: the Master's in-memory registry of Workers.
// It is the single place that knows who is alive, how loaded they are and
// which socket to use to reach them. State changes are mirrored to PostgreSQL.
#pragma once

#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "../common/message.h"
#include "../database/database.h"

namespace nexus {

struct WorkerInfo {
    std::string id, hostname, ip;
    int cpuCores = 0;
    long long memoryMb = 0;
    double cpu = 0, ram = 0, disk = 0;          // latest reported usage, percent
    bool active = false;
    int runningTasks = 0;                       // tasks assigned but not finished
    std::chrono::steady_clock::time_point lastHeartbeat;
    std::shared_ptr<Connection> conn;
};

class WorkerManager {
public:
    explicit WorkerManager(Database& db) : db_(db) {}

    bool registerWorker(const Message& reg, std::shared_ptr<Connection> conn);
    void onHeartbeat(const std::string& id);
    void onResourceUpdate(const std::string& id, double cpu, double ram, double disk);
    // Called when a Worker's socket closes. Returns true if this connection was
    // the Worker's current one (a stale connection after re-register returns false).
    bool onDisconnect(const std::string& id, const Connection* conn);
    // Marks Workers silent for longer than `timeoutSec` INACTIVE.
    void checkTimeouts(int timeoutSec);

    std::vector<WorkerInfo> activeSnapshot() const;
    std::shared_ptr<Connection> connectionOf(const std::string& id) const;
    void adjustRunning(const std::string& id, int delta);
    void disconnectAll();

private:
    Database& db_;
    mutable std::mutex mutex_;
    std::map<std::string, WorkerInfo> workers_;
};

}  // namespace nexus
