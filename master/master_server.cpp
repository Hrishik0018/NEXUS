#include "master_server.h"

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdio>

#include "../common/config.h"
#include "../common/log.h"
#include "../common/protocol.h"

namespace nexus {

namespace {
// Set NEXUS_QUIET_HEARTBEAT=1 to hide the per-heartbeat lines.
bool quietHeartbeat() {
    static const bool q = envOr("NEXUS_QUIET_HEARTBEAT", "0") == "1";
    return q;
}
}  // namespace
// Starts the TCP listener and launches separate threads for accepting
// worker connections and monitoring worker heartbeat timeouts.
bool MasterServer::start(std::string& err) {
    listenFd_ = createListener(port_, err);
    if (listenFd_ < 0) return false;
    running_ = true;
    acceptThread_  = std::thread(&MasterServer::acceptLoop, this);
    monitorThread_ = std::thread(&MasterServer::monitorLoop, this);
    return true;
}
// Stops the server, disconnects workers, joins background threads,
// and closes the listening socket.
void MasterServer::stop() {
    if (!running_.exchange(false)) return;
    workers_.disconnectAll();                 // wake every blocked reader thread
    if (acceptThread_.joinable()) acceptThread_.join();
    if (monitorThread_.joinable()) monitorThread_.join();
    std::vector<std::thread> threads;
    {
        std::lock_guard<std::mutex> lock(threadsMutex_);
        threads.swap(connThreads_);
    }
    for (auto& t : threads) if (t.joinable()) t.join();
    if (listenFd_ >= 0) { ::close(listenFd_); listenFd_ = -1; }
}

void MasterServer::acceptLoop() {
    while (running_) {
        pollfd p;
        p.fd = listenFd_; p.events = POLLIN; p.revents = 0;
        if (::poll(&p, 1, 500) <= 0) continue;     // timeout lets us notice shutdown

        int fd = ::accept(listenFd_, nullptr, nullptr);
        if (fd < 0) continue;
        auto conn = std::make_shared<Connection>(fd);
        std::lock_guard<std::mutex> lock(threadsMutex_);
        connThreads_.emplace_back(&MasterServer::handleConnection, this, conn);
    }
}

// Failure detection: a Worker that stops heartbeating is marked INACTIVE so the
// scheduler no longer considers it. (Re-scheduling its tasks is a later phase.)
void MasterServer::monitorLoop() {
    const int timeout = heartbeatSec_ * proto::HEARTBEAT_MISS_FACTOR;
    while (running_) {
        for (int i = 0; i < 10 && running_; ++i) std::this_thread::sleep_for(std::chrono::milliseconds(100));
        workers_.checkTimeouts(timeout);
    }
}

void MasterServer::handleConnection(std::shared_ptr<Connection> conn) {
    // The first message on every connection must be REGISTER_WORKER.
    Message reg;
    if (conn->receive(reg, 10000) != 1 || reg.type() != proto::REGISTER_WORKER ||
        reg.get("worker_id").empty()) {
        log("MASTER", "Rejected connection from " + conn->peerAddress() + " (no valid registration)");
        return;
    }
    const std::string id = reg.get("worker_id");
    log("MASTER", "Worker registration received: " + id);

    if (!workers_.registerWorker(reg, conn)) {
        log("MASTER", "Registration of " + id + " failed (database error)");
        return;
    }
    Message ack(proto::REGISTER_ACK);
    ack.set("status", "OK").set("worker_id", id).setInt("heartbeat_sec", heartbeatSec_);
    conn->send(ack);
    log("MASTER", "Worker " + id + " registered successfully (" + reg.get("hostname") + ", " +
                  reg.get("cpu_cores") + " cores, " + reg.get("memory_mb") + " MB RAM)");

    Message m;
    while (running_) {
        int r = conn->receive(m, 1000);
        if (r < 0) break;
        if (r == 0) continue;

        const std::string& t = m.type();
        if (t == proto::HEARTBEAT) {
            workers_.onHeartbeat(id);
            if (!quietHeartbeat()) log("MASTER", "Heartbeat received from " + id);
        } else if (t == proto::RESOURCE_UPDATE) {
            double cpu = m.getDouble("cpu"), ram = m.getDouble("ram"), disk = m.getDouble("disk");
            workers_.onResourceUpdate(id, cpu, ram, disk);
            if (!quietHeartbeat()) {
                char buf[96];
                std::snprintf(buf, sizeof buf, "Resources from %s: CPU %.0f%% | RAM %.0f%% | DISK %.0f%%",
                              id.c_str(), cpu, ram, disk);
                log("MASTER", buf);
            }
        } else if (t == proto::TASK_STARTED) {
            tasks_.onTaskStarted(m);
        } else if (t == proto::TASK_RESULT) {
            tasks_.onTaskResult(id, m);
        } else if (t == proto::TASK_FAILURE) {
            tasks_.onTaskFailure(id, m);
        } else {
            log("MASTER", "Unknown message type '" + t + "' from " + id);
        }
    }

    if (workers_.onDisconnect(id, conn.get())) {
        log("MASTER", "Worker " + id + " disconnected -> marked INACTIVE");
        tasks_.failTasksOfWorker(id, "worker disconnected");
    }
}

}  // namespace nexus
