#include "scheduler.h"

#include <algorithm>
#include <cstdio>

namespace nexus {

double Scheduler::loadScore(const WorkerInfo& w) {
    // CPU dominates; RAM/disk break near-ties; queued tasks stop the Master from
    // piling every task on one Worker before its CPU reading has caught up.
    return 0.6 * w.cpu + 0.3 * w.ram + 0.1 * w.disk + 10.0 * w.runningTasks;
}

SchedulingDecision Scheduler::select(const std::string& taskLabel,
                                     const std::vector<WorkerInfo>& activeWorkers) const {
    SchedulingDecision d;
    d.policy = kPolicyName;
    if (activeWorkers.empty()) {
        d.report = "Task: " + taskLabel + "\n\n  No active Workers available.";
        return d;
    }

    std::vector<WorkerInfo> ws = activeWorkers;
    std::sort(ws.begin(), ws.end(),
              [](const WorkerInfo& a, const WorkerInfo& b) { return a.id < b.id; });

    const WorkerInfo* best = &ws[0];
    for (const auto& w : ws)
        if (loadScore(w) < loadScore(*best)) best = &w;

    d.found    = true;
    d.workerId = best->id;
    d.cpu      = best->cpu;
    d.ram      = best->ram;

    d.report = "Task: " + taskLabel + "   Policy: " + kPolicyName + "\n\n";
    char line[160];
    for (const auto& w : ws) {
        std::snprintf(line, sizeof line,
                      "  %-4s -> CPU %3.0f%%  RAM %3.0f%%  queued %d  score %5.1f%s\n",
                      w.id.c_str(), w.cpu, w.ram, w.runningTasks, loadScore(w),
                      &w == best ? "   <-- SELECTED" : "");
        d.report += line;
    }
    d.report += "\nAssigned Worker: " + best->id;
    return d;
}

}  // namespace nexus
