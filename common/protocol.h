
#pragma once
#include <string>
namespace nexus {
namespace proto {


// Worker -> Master
constexpr const char* REGISTER_WORKER = "REGISTER_WORKER";
constexpr const char* HEARTBEAT       = "HEARTBEAT";
constexpr const char* RESOURCE_UPDATE = "RESOURCE_UPDATE";
constexpr const char* TASK_STARTED    = "TASK_STARTED";
constexpr const char* TASK_RESULT     = "TASK_RESULT";
constexpr const char* TASK_FAILURE    = "TASK_FAILURE";
// Master -> Worker
constexpr const char* REGISTER_ACK    = "REGISTER_ACK";
constexpr const char* TASK_ASSIGN     = "TASK_ASSIGN";

// ---- Defaults (overridable with environment variables) -------------------
constexpr int DEFAULT_MASTER_PORT      = 5050;
constexpr int DEFAULT_HEARTBEAT_SEC    = 3;
// A Worker silent for this many heartbeat intervals is marked INACTIVE.
constexpr int HEARTBEAT_MISS_FACTOR    = 3;

// ---- Task types understood by the prototype ------------------------------
inline bool isKnownTaskType(const std::string& t) {
    return t == "WORD_COUNT" || t == "PRIME_COUNT" ||
           t == "MATRIX_MULTIPLY" || t == "CHECKSUM";
}

}  // namespace proto
}  // namespace nexus
