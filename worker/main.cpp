// NEXUS Worker.   Usage:  ./worker <WORKER_ID> [master_host] [master_port]
#include <csignal>
#include <cstdio>
#include <cstdlib>

#include "../common/config.h"
#include "../common/log.h"
#include "../common/protocol.h"
#include "worker_client.h"

using namespace nexus;

namespace {
WorkerClient* g_client = nullptr;
void onSignal(int) { if (g_client) g_client->stop(); }
}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: %s <WORKER_ID> [master_host] [master_port]\n"
                             "  (defaults come from NEXUS_MASTER_HOST / NEXUS_MASTER_PORT)\n", argv[0]);
        return 2;
    }
    std::signal(SIGPIPE, SIG_IGN);

    WorkerClient::Config cfg;
    cfg.workerId     = argv[1];
    cfg.masterHost   = argc > 2 ? argv[2] : envOr("NEXUS_MASTER_HOST", "127.0.0.1");
    cfg.masterPort   = argc > 3 ? std::atoi(argv[3]) : envIntOr("NEXUS_MASTER_PORT", proto::DEFAULT_MASTER_PORT);
    cfg.heartbeatSec = envIntOr("NEXUS_HEARTBEAT_SEC", proto::DEFAULT_HEARTBEAT_SEC);

    WorkerClient client(cfg);
    g_client = &client;
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    return client.run();
}
