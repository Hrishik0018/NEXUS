#include "task_manager.h"

#include <cstdio>
#include <vector>

#include "../common/log.h"
#include "../common/protocol.h"

namespace nexus {

std::string TaskManager::label(long long taskId) {
    char buf[32];
    std::snprintf(buf, sizeof buf, "TASK-%03lld", taskId);
    return buf;
}

bool TaskManager::takeInFlight(long long taskId, InFlight& out) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = inFlight_.find(taskId);
    if (it == inFlight_.end()) return false;
    out = it->second;
    inFlight_.erase(it);
    return true;
}

bool TaskManager::submit(const std::string& type, int scale, std::string& err) {
    if (!proto::isKnownTaskType(type)) {
        err = "unknown task type '" + type + "' (WORD_COUNT, PRIME_COUNT, MATRIX_MULTIPLY, CHECKSUM)";
        return false;
    }
    if (scale < 1) scale = 1;

    // 1. Persist the job and its task first: PostgreSQL is the source of truth.
    long long jobId = 0, taskId = 0;
    if (!db_.createJobAndTask(type, "scale=" + std::to_string(scale), jobId, taskId, err)) {
        err = "database error: " + err;
        return false;
    }
    const std::string lbl = label(taskId);
    log("MASTER", "Task submitted: " + lbl + " (job " + std::to_string(jobId) + ", " + type +
                  ", scale " + std::to_string(scale) + ")");

    // 2. Ask the scheduler which active Worker should run it.
    SchedulingDecision d = scheduler_.select(lbl, workers_.activeSnapshot());
    log("SCHEDULER", d.report);
    if (!d.found) {
        err = lbl + " stays PENDING: no active Workers";
        return false;
    }

    // 3. Remember the task BEFORE sending, so a very fast result can't race us.
    InFlight f;
    f.taskId = taskId; f.type = type; f.workerId = d.workerId; f.policy = d.policy;
    f.cpuAtAssign = d.cpu; f.ramAtAssign = d.ram;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        inFlight_[taskId] = f;
    }
    workers_.adjustRunning(d.workerId, +1);
    db_.markTaskAssigned(taskId, d.workerId);

    // 4. Send the real TASK_ASSIGN message over TCP.
    Message m(proto::TASK_ASSIGN);
    m.setInt("task_id", taskId).set("task_label", lbl).set("task_type", type).setInt("scale", scale);
    auto conn = workers_.connectionOf(d.workerId);
    if (!conn || !conn->send(m)) {
        err = "could not reach " + d.workerId;
        InFlight dummy;
        if (takeInFlight(taskId, dummy)) {
            workers_.adjustRunning(d.workerId, -1);
            db_.markTaskFailed(taskId, "send to worker failed");
            db_.recordExecution(taskId, d.workerId, type, 0, "FAILED", d.cpu, d.ram, d.policy);
        }
        return false;
    }
    log("MASTER", lbl + " assigned to " + d.workerId);
    return true;
}

void TaskManager::onTaskStarted(const Message& m) {
    db_.markTaskStarted(m.getInt("task_id"));
}

void TaskManager::onTaskResult(const std::string& workerId, const Message& m) {
    long long taskId = m.getInt("task_id");
    double execSec = m.getDouble("exec_time");
    InFlight f;
    if (!takeInFlight(taskId, f)) {
        log("MASTER", "Ignoring result for unknown " + label(taskId));
        return;
    }
    workers_.adjustRunning(workerId, -1);
    db_.markTaskCompleted(taskId, m.get("result"));
    db_.recordExecution(taskId, workerId, f.type, execSec, "COMPLETED",
                        f.cpuAtAssign, f.ramAtAssign, f.policy);

    char buf[64];
    std::snprintf(buf, sizeof buf, "%.2f", execSec);
    log("MASTER", label(taskId) + " completed successfully on " + workerId + "\n" +
                  "  execution time: " + buf + " s\n  result: " + m.get("result"));
}

void TaskManager::onTaskFailure(const std::string& workerId, const Message& m) {
    long long taskId = m.getInt("task_id");
    InFlight f;
    if (!takeInFlight(taskId, f)) return;
    workers_.adjustRunning(workerId, -1);
    db_.markTaskFailed(taskId, m.get("error"));
    db_.recordExecution(taskId, workerId, f.type, m.getDouble("exec_time"), "FAILED",
                        f.cpuAtAssign, f.ramAtAssign, f.policy);
    log("MASTER", label(taskId) + " FAILED on " + workerId + ": " + m.get("error"));
}

void TaskManager::failTasksOfWorker(const std::string& workerId, const std::string& reason) {
    std::vector<InFlight> lost;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto it = inFlight_.begin(); it != inFlight_.end();) {
            if (it->second.workerId == workerId) { lost.push_back(it->second); it = inFlight_.erase(it); }
            else ++it;
        }
    }
    // Future work: re-schedule these on another Worker instead of failing them.
    for (const auto& f : lost) {
        db_.markTaskFailed(f.taskId, reason);
        db_.recordExecution(f.taskId, workerId, f.type, 0, "FAILED", f.cpuAtAssign, f.ramAtAssign, f.policy);
        log("MASTER", label(f.taskId) + " FAILED: " + reason);
    }
}

}  // namespace nexus
