// Thread-safe console logger. Every thread (socket handlers, heartbeat loop,
// CLI) prints through this so lines never interleave mid-message.
#pragma once

#include <cstdio>
#include <mutex>
#include <string>

namespace nexus {

// log("MASTER", "hello")           ->  [MASTER] hello
// log("SCHEDULER", "line1\nline2") ->  [SCHEDULER]
//                                      line1
//                                      line2
inline void log(const std::string& tag, const std::string& msg) {
    static std::mutex m;
    std::lock_guard<std::mutex> lock(m);
    if (msg.find('\n') == std::string::npos)
        std::printf("[%s] %s\n", tag.c_str(), msg.c_str());
    else
        std::printf("[%s]\n%s\n", tag.c_str(), msg.c_str());
    std::fflush(stdout);
}

}  // namespace nexus
