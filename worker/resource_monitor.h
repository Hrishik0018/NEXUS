// ResourceMonitor: real CPU / RAM / disk readings from the operating system.
//   CPU  : delta of the aggregate counters in /proc/stat between two samples
//   RAM  : /proc/meminfo  (MemTotal - MemAvailable)
//   Disk : statvfs() of the Worker's storage directory
//   Load : /proc/loadavg ; process memory: /proc/self/statm
// This MONITORS the OS; it does not allocate, limit or reserve OS resources.
// Linux only (WSL included). On other platforms supported() is false and the
// Worker refuses to start with an explicit message.
#pragma once

#include <chrono>
#include <mutex>
#include <string>

namespace nexus {

struct ResourceSample {
    double cpuPercent = 0;
    double ramPercent = 0;
    double diskPercent = 0;
    long long ramAvailableMb = 0;
    double load1 = 0;            // 1-minute load average
    double processRssMb = 0;     // resident memory of this Worker process
    double processCpuPercent = 0; // CPU used by THIS Worker process only, as % of the CPUs it may use
    std::string busyCores;        // cores that had a running thread of this Worker at sampling time, e.g. "1" or "0,2"
};

class ResourceMonitor {
public:
    explicit ResourceMonitor(std::string diskPath = "/");   // takes the first /proc/stat baseline

    static bool supported();                 // false off Linux
    ResourceSample sample();                 // CPU % is "since the previous call"
    int cpuCores() const;
    long long totalMemoryMb() const;
    static std::string runningCores();       // cores of this process's threads that are in state R right now

private:
    struct CpuTimes { unsigned long long busy = 0, total = 0; bool valid = false; };
    static CpuTimes readCpuTimes();

    std::string diskPath_;
    std::mutex mutex_;
    CpuTimes prev_;
    double prevProcSec_ = 0;                 // this process's CPU seconds at the previous sample
    std::chrono::steady_clock::time_point prevProcAt_;
    bool procValid_ = false;
};

}  // namespace nexus
