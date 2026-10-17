#include "worker_manager.h"

#include "../common/log.h"

namespace nexus {

using Clock = std::chrono::steady_clock;

bool WorkerManager::registerWorker(const Message& reg, std::shared_ptr<Connection> conn) {
    WorkerInfo w;
    w.id       = reg.get("worker_id");
    w.hostname = reg.get("hostname");
    w.ip       = conn->peerAddress();
    w.cpuCores = static_cast<int>(reg.getInt("cpu_cores"));
    w.memoryMb = reg.getInt("memory_mb");
    w.active   = true;
    w.lastHeartbeat = Clock::now();
    w.conn     = conn;

    if (!db_.upsertWorker(w.id, w.hostname, w.ip, w.cpuCores, w.memoryMb)) return false;

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = workers_.find(w.id);
    if (it != workers_.end()) w.runningTasks = it->second.runningTasks;  // keep counters on re-register
    workers_[w.id] = w;
    return true;
}

void WorkerManager::onHeartbeat(const std::string& id) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = workers_.find(id);
        if (it == workers_.end()) return;
        it->second.lastHeartbeat = Clock::now();
        it->second.active = true;       // a Worker that was marked INACTIVE comes back
    }
    db_.recordHeartbeat(id);
}

void WorkerManager::onResourceUpdate(const std::string& id, double cpu, double ram, double disk) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = workers_.find(id);
        if (it == workers_.end()) return;
        it->second.cpu = cpu;
        it->second.ram = ram;
        it->second.disk = disk;
        it->second.lastHeartbeat = Clock::now();
        it->second.active = true;
    }
    db_.recordResources(id, cpu, ram, disk);
}

bool WorkerManager::onDisconnect(const std::string& id, const Connection* conn) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = workers_.find(id);
        if (it == workers_.end() || it->second.conn.get() != conn) return false;
        it->second.active = false;
        it->second.runningTasks = 0;
        it->second.conn.reset();
    }
    db_.setWorkerStatus(id, "INACTIVE");
    return true;
}

void WorkerManager::checkTimeouts(int timeoutSec) {
    std::vector<std::string> expired;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto now = Clock::now();
        for (auto& kv : workers_) {
            WorkerInfo& w = kv.second;
            if (w.active && now - w.lastHeartbeat > std::chrono::seconds(timeoutSec)) {
                w.active = false;
                expired.push_back(w.id);
            }
        }
    }
    for (const auto& id : expired) {
        log("MASTER", "Worker " + id + " missed heartbeats -> marked INACTIVE");
        db_.setWorkerStatus(id, "INACTIVE");
    }
}

std::vector<WorkerInfo> WorkerManager::activeSnapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<WorkerInfo> out;
    for (const auto& kv : workers_)
        if (kv.second.active && kv.second.conn) out.push_back(kv.second);
    return out;
}

std::shared_ptr<Connection> WorkerManager::connectionOf(const std::string& id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = workers_.find(id);
    return it == workers_.end() ? nullptr : it->second.conn;
}

void WorkerManager::adjustRunning(const std::string& id, int delta) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = workers_.find(id);
    if (it == workers_.end()) return;
    it->second.runningTasks += delta;
    if (it->second.runningTasks < 0) it->second.runningTasks = 0;
}

void WorkerManager::disconnectAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& kv : workers_)
        if (kv.second.conn) kv.second.conn->shutdown();
}

}  // namespace nexus
