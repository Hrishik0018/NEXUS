#include "worker_client.h"

#include <unistd.h>

#include <chrono>
#include <cstdio>

#include "../common/log.h"
#include "../common/protocol.h"
#include "task_executor.h"

namespace nexus {

void WorkerClient::stop() {
    running_ = false;
    wake_.notify_all();
    if (conn_) conn_->shutdown();
}

bool WorkerClient::connectAndRegister() {
    log(tag(), "Connecting to Master at " + cfg_.masterHost + ":" + std::to_string(cfg_.masterPort) + "...");

    // The Master may not be up yet: retry for a while instead of failing at once.
    std::string err;
    for (int attempt = 1; attempt <= 10 && running_; ++attempt) {
        conn_ = Connection::connectTo(cfg_.masterHost, cfg_.masterPort, err);
        if (conn_) break;
        log(tag(), "Connect failed (" + err + "), retry " + std::to_string(attempt) + "/10");
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
    if (!conn_) return false;

    char host[256] = "unknown";
    ::gethostname(host, sizeof host);

    Message reg(proto::REGISTER_WORKER);
    reg.set("worker_id", cfg_.workerId)
       .set("hostname", host)
       .setInt("cpu_cores", monitor_.cpuCores())
       .setInt("memory_mb", monitor_.totalMemoryMb());
    if (!conn_->send(reg)) { log(tag(), "Could not send registration"); return false; }

    Message ack;
    if (conn_->receive(ack, 10000) != 1 || ack.type() != proto::REGISTER_ACK ||
        ack.get("status") != "OK") {
        log(tag(), "Registration rejected or timed out");
        return false;
    }
    log(tag(), "Registration successful");
    return true;
}

void WorkerClient::heartbeatLoop() {
    unsigned long long seq = 0;
    while (running_) {
        // Heartbeat first: proves liveness even if resource sampling were slow.
        Message hb(proto::HEARTBEAT);
        hb.set("worker_id", cfg_.workerId).setInt("seq", static_cast<long long>(++seq));
        if (!conn_->send(hb)) break;
        log(tag(), "HEARTBEAT");

        ResourceSample s = monitor_.sample();
        Message ru(proto::RESOURCE_UPDATE);
        ru.set("worker_id", cfg_.workerId)
          .setDouble("cpu", s.cpuPercent).setDouble("ram", s.ramPercent).setDouble("disk", s.diskPercent);
        if (!conn_->send(ru)) break;
        char buf[96];
        std::snprintf(buf, sizeof buf, "CPU: %.0f%% | RAM: %.0f%% | DISK: %.0f%%",
                      s.cpuPercent, s.ramPercent, s.diskPercent);
        log(tag(), buf);

        std::unique_lock<std::mutex> lock(wakeMutex_);
        wake_.wait_for(lock, std::chrono::seconds(cfg_.heartbeatSec), [this] { return !running_.load(); });
    }
}

void WorkerClient::executeTask(Message assign) {
    const long long taskId = assign.getInt("task_id");
    const std::string lbl  = assign.get("task_label");
    const std::string type = assign.get("task_type");
    const int scale        = static_cast<int>(assign.getInt("scale", 1));

    log(tag(), "Task received: " + lbl + "\nTask type: " + type + "\nExecuting...");
    Message started(proto::TASK_STARTED);
    started.setInt("task_id", taskId).set("worker_id", cfg_.workerId);
    conn_->send(started);

    TaskOutcome out = TaskExecutor::run(type, scale);

    if (out.ok) {
        char buf[64];
        std::snprintf(buf, sizeof buf, "%.2f", out.seconds);
        log(tag(), "Task completed\nExecution time: " + std::string(buf) + " seconds\nResult: " + out.result);
        Message res(proto::TASK_RESULT);
        res.setInt("task_id", taskId).set("worker_id", cfg_.workerId)
           .setDouble("exec_time", out.seconds).set("result", out.result);
        log(tag(), "Sending TASK_RESULT");
        conn_->send(res);
    } else {
        log(tag(), "Task FAILED: " + out.error);
        Message fail(proto::TASK_FAILURE);
        fail.setInt("task_id", taskId).set("worker_id", cfg_.workerId)
            .setDouble("exec_time", out.seconds).set("error", out.error);
        conn_->send(fail);
    }
}

int WorkerClient::run() {
    running_ = true;
    if (!connectAndRegister()) { running_ = false; return 1; }

    std::thread hbThread(&WorkerClient::heartbeatLoop, this);

    Message m;
    while (running_) {
        int r = conn_->receive(m, 1000);
        if (r < 0) { log(tag(), "Connection to Master lost"); break; }
        if (r == 0) continue;
        if (m.type() == proto::TASK_ASSIGN) {
            std::lock_guard<std::mutex> lock(taskThreadsMutex_);
            taskThreads_.emplace_back(&WorkerClient::executeTask, this, m);
        }
    }

    running_ = false;
    wake_.notify_all();
    conn_->shutdown();
    hbThread.join();
    {
        std::lock_guard<std::mutex> lock(taskThreadsMutex_);
        for (auto& t : taskThreads_) if (t.joinable()) t.join();
    }
    return 0;
}

}  // namespace nexus
