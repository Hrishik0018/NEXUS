// Scheduler: baseline LEAST-LOADED / resource-aware policy.
//
// This is deliberately NOT "AI". It is the baseline that the later
// execution-history-trained (XGBoost) scheduler will be compared against.
// Keeping it behind its own class means the policy can be swapped later
// (Round Robin, AI-assisted ...) without touching the rest of the Master.
#pragma once

#include <string>
#include <vector>

#include "worker_manager.h"

namespace nexus {

struct SchedulingDecision {
    bool found = false;
    std::string workerId;
    double cpu = 0, ram = 0;       // load of the chosen Worker at decision time
    std::string policy;
    std::string report;            // formatted table for the terminal
};

class Scheduler {
public:
    static constexpr const char* kPolicyName = "LEAST_LOADED";

    // load score = 0.6*CPU% + 0.3*RAM% + 0.1*DISK% + 10 per task already queued
    // on that Worker. Lowest score wins; ties go to the lower Worker id.
    static double loadScore(const WorkerInfo& w);

    SchedulingDecision select(const std::string& taskLabel,
                              const std::vector<WorkerInfo>& activeWorkers) const;
};

}  // namespace nexus
