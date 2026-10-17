// TaskManager: owns the life cycle of a task on the Master side.
//   submit -> (PostgreSQL job+task) -> scheduler -> TASK_ASSIGN -> Worker
//   TASK_STARTED / TASK_RESULT / TASK_FAILURE <- Worker -> PostgreSQL history
#pragma once

#include <map>
#include <mutex>
#include <string>

#include "../common/message.h"
#include "../database/database.h"
#include "scheduler.h"
#include "worker_manager.h"

namespace nexus {

class TaskManager {
public:
    TaskManager(Database& db, WorkerManager& workers, Scheduler& scheduler)
        : db_(db), workers_(workers), scheduler_(scheduler) {}

    // Creates job+task in PostgreSQL, schedules, and sends TASK_ASSIGN.
    bool submit(const std::string& type, int scale, std::string& err);

    void onTaskStarted(const Message& m);
    void onTaskResult(const std::string& workerId, const Message& m);
    void onTaskFailure(const std::string& workerId, const Message& m);
    // A Worker vanished: every task still in flight on it is failed.
    void failTasksOfWorker(const std::string& workerId, const std::string& reason);

    static std::string label(long long taskId);   // 7 -> "TASK-007"

private:
    struct InFlight {
        long long taskId = 0;
        std::string type, workerId, policy;
        double cpuAtAssign = 0, ramAtAssign = 0;
    };
    bool takeInFlight(long long taskId, InFlight& out);

    Database& db_;
    WorkerManager& workers_;
    Scheduler& scheduler_;
    std::mutex mutex_;
    std::map<long long, InFlight> inFlight_;
};

}  // namespace nexus
