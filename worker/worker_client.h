// WorkerClient: connects to the Master, registers, then runs
//   - a heartbeat/resource thread (HEARTBEAT + RESOURCE_UPDATE every N s)
//   - a receive loop (main thread) that executes TASK_ASSIGN messages
// Tasks run on their own threads so heartbeats keep flowing during execution.
#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "../common/message.h"
#include "resource_monitor.h"

namespace nexus {

class WorkerClient {
public:
    struct Config {
        std::string workerId;
        std::string masterHost;
        int masterPort;
        int heartbeatSec;
    };

    explicit WorkerClient(Config cfg) : cfg_(std::move(cfg)) {}

    int run();                    // blocks until the Master goes away or stop()
    void stop();

private:
    bool connectAndRegister();
    void heartbeatLoop();
    void executeTask(Message assign);
    std::string tag() const { return "WORKER " + cfg_.workerId; }

    Config cfg_;
    ResourceMonitor monitor_;
    std::shared_ptr<Connection> conn_;
    std::atomic<bool> running_{false};
    std::mutex wakeMutex_;
    std::condition_variable wake_;
    std::mutex taskThreadsMutex_;
    std::vector<std::thread> taskThreads_;
};

}  // namespace nexus
