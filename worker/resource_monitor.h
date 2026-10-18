// ResourceMonitor: real CPU / RAM / disk readings from the operating system.
//   CPU  : delta of the aggregate counters in /proc/stat between two samples
//   RAM  : /proc/meminfo  (MemTotal - MemAvailable)
//   Disk : statvfs("/")   (used / total blocks of the root filesystem)
#pragma once

#include <mutex>

namespace nexus {

struct ResourceSample {
    double cpuPercent = 0;
    double ramPercent = 0;
    double diskPercent = 0;
};

class ResourceMonitor {
public:
    ResourceMonitor();                       // takes the first /proc/stat baseline

    ResourceSample sample();                 // CPU % is "since the previous call"
    int cpuCores() const;
    long long totalMemoryMb() const;

private:
    struct CpuTimes { unsigned long long busy = 0, total = 0; bool valid = false; };
    static CpuTimes readCpuTimes();

    std::mutex mutex_;
    CpuTimes prev_;
};

}  // namespace nexus
