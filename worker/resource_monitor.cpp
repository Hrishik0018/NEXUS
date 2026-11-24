#include "resource_monitor.h"

#include <sys/statvfs.h>
#include <dirent.h>
#include <sched.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace nexus {

namespace {

// Reads "Key:   12345 kB" lines from /proc/meminfo; returns kB or -1.
long long meminfoKb(const std::string& wanted) {
    std::ifstream f("/proc/meminfo");
    std::string key;
    long long value;
    std::string unit;
    while (f >> key >> value) {
        std::getline(f, unit);                       // rest of line ("kB")
        if (key == wanted + ":") return value;
    }
    return -1;
}

// CPU seconds (user + system) used so far by this whole process, from /proc/self/stat.
double processCpuSeconds() {
    std::ifstream f("/proc/self/stat");
    std::string line;
    if (!std::getline(f, line)) return -1;
    size_t p = line.rfind(')');                       // the command name may contain spaces
    if (p == std::string::npos) return -1;
    std::istringstream in(line.substr(p + 2));
    std::string tok;
    unsigned long long utime = 0, stime = 0;
    for (int i = 3; i <= 15 && (in >> tok); ++i) {    // field 14 = utime, 15 = stime
        if (i == 14) utime = std::stoull(tok);
        if (i == 15) stime = std::stoull(tok);
    }
    long hz = ::sysconf(_SC_CLK_TCK);
    return hz > 0 ? static_cast<double>(utime + stime) / static_cast<double>(hz) : -1;
}

}  // namespace

// Which CPU core is each running thread of this Worker on right now? /proc/self/task/<tid>/stat:
// field 3 = state (R = running), field 39 = the CPU the thread last ran on. The calling thread is skipped.
std::string ResourceMonitor::runningCores() {
    std::vector<int> cores;
    const long self = ::syscall(SYS_gettid);
    if (DIR* d = ::opendir("/proc/self/task")) {
        while (dirent* e = ::readdir(d)) {
            if (e->d_name[0] < '0' || e->d_name[0] > '9') continue;
            if (std::atol(e->d_name) == self) continue;
            std::ifstream f(std::string("/proc/self/task/") + e->d_name + "/stat");
            std::string line;
            if (!std::getline(f, line)) continue;
            size_t p = line.rfind(')');
            if (p == std::string::npos) continue;
            std::istringstream in(line.substr(p + 2));
            std::string tok, state;
            int cpu = -1;
            for (int i = 3; i <= 39 && (in >> tok); ++i) {
                if (i == 3) state = tok;
                if (i == 39) cpu = std::atoi(tok.c_str());
            }
            if (state == "R" && cpu >= 0) cores.push_back(cpu);
        }
        ::closedir(d);
    }
    std::sort(cores.begin(), cores.end());
    cores.erase(std::unique(cores.begin(), cores.end()), cores.end());
    std::string s;
    for (int c : cores) s += (s.empty() ? "" : ",") + std::to_string(c);
    return s;
}

bool ResourceMonitor::supported() {
#if defined(__linux__)
    return true;
#else
    return false;
#endif
}

ResourceMonitor::CpuTimes ResourceMonitor::readCpuTimes() {
    CpuTimes t;
    std::ifstream f("/proc/stat");
    std::string label;
    if (!(f >> label) || label != "cpu") return t;
    // user nice system idle iowait irq softirq steal ...
    unsigned long long v[8] = {0};
    for (int i = 0; i < 8 && (f >> v[i]); ++i) {}
    const unsigned long long idle = v[3] + v[4];     // idle + iowait
    unsigned long long total = 0;
    for (int i = 0; i < 8; ++i) total += v[i];
    t.total = total;
    t.busy = total - idle;
    t.valid = total > 0;
    return t;
}

ResourceMonitor::ResourceMonitor(std::string diskPath) : diskPath_(std::move(diskPath)) {
    prev_ = readCpuTimes();
    prevProcSec_ = processCpuSeconds();
    prevProcAt_ = std::chrono::steady_clock::now();
    procValid_ = prevProcSec_ >= 0;
    // Give the first sample() a real interval to measure over.
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
}

ResourceSample ResourceMonitor::sample() {
    ResourceSample s;

    // ---- CPU: busy-time fraction since the previous sample -------------
    {
        std::lock_guard<std::mutex> lock(mutex_);
        CpuTimes now = readCpuTimes();
        if (now.valid && prev_.valid && now.total > prev_.total) {
            double busy  = static_cast<double>(now.busy - prev_.busy);
            double total = static_cast<double>(now.total - prev_.total);
            s.cpuPercent = std::min(100.0, std::max(0.0, 100.0 * busy / total));
        }
        if (now.valid) prev_ = now;
    }

    // ---- CPU used by this Worker process only (not the whole host) -------
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const double nowSec = processCpuSeconds();
        const auto nowAt = std::chrono::steady_clock::now();
        if (procValid_ && nowSec >= 0) {
            const double wall = std::chrono::duration<double>(nowAt - prevProcAt_).count();
            cpu_set_t set;
            CPU_ZERO(&set);
            int usable = (::sched_getaffinity(0, sizeof set, &set) == 0) ? CPU_COUNT(&set) : 1;
            if (usable < 1) usable = 1;
            if (wall > 0.05)
                s.processCpuPercent = std::min(100.0, std::max(0.0, 100.0 * (nowSec - prevProcSec_) / (wall * usable)));
        }
        if (nowSec >= 0) { prevProcSec_ = nowSec; prevProcAt_ = nowAt; procValid_ = true; }
    }
    s.busyCores = runningCores();

    // ---- RAM -----------------------------------------------------------
    long long total = meminfoKb("MemTotal");
    long long avail = meminfoKb("MemAvailable");
    if (avail < 0) {                                  // older kernels: approximate
        long long f = meminfoKb("MemFree"), b = meminfoKb("Buffers"), c = meminfoKb("Cached");
        if (f >= 0) avail = f + std::max<long long>(b, 0) + std::max<long long>(c, 0);
    }
    if (total > 0 && avail >= 0) {
        s.ramPercent = std::min(100.0, std::max(0.0, 100.0 * (total - avail) / total));
        s.ramAvailableMb = avail / 1024;
    }

    // ---- Disk (filesystem that holds the Worker's storage) --------------
    struct statvfs fs;
    if (::statvfs(diskPath_.c_str(), &fs) == 0 && fs.f_blocks > 0) {
        double used = static_cast<double>(fs.f_blocks - fs.f_bfree);
        double usable = used + static_cast<double>(fs.f_bavail);   // same maths as `df`
        if (usable > 0) s.diskPercent = std::min(100.0, 100.0 * used / usable);
    }

    // ---- load average + this process's resident memory ------------------
    {
        std::ifstream f("/proc/loadavg");
        double l1 = 0;
        if (f >> l1) s.load1 = l1;
    }
    {
        std::ifstream f("/proc/self/statm");
        long long pages = 0, rss = 0;
        if (f >> pages >> rss) s.processRssMb = static_cast<double>(rss) * static_cast<double>(::sysconf(_SC_PAGESIZE)) / (1024.0 * 1024.0);
    }
    return s;
}

int ResourceMonitor::cpuCores() const {
    long n = ::sysconf(_SC_NPROCESSORS_ONLN);
    return n > 0 ? static_cast<int>(n) : 1;
}

long long ResourceMonitor::totalMemoryMb() const {
    long long kb = meminfoKb("MemTotal");
    return kb > 0 ? kb / 1024 : 0;
}

}  // namespace nexus
