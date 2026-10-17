// NEXUS Master: TCP server + scheduler + interactive task-submission console.
#include <poll.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <csignal>
#include <cstdio>
#include <iostream>
#include <sstream>

#include "../common/config.h"
#include "../common/log.h"
#include "../common/protocol.h"
#include "../database/database.h"
#include "master_server.h"
#include "scheduler.h"
#include "task_manager.h"
#include "worker_manager.h"

using namespace nexus;

namespace {

std::atomic<bool> g_stop{false};
void onSignal(int) { g_stop = true; }

void printTable(const Database::Result& r) {
    if (!r.ok) { std::printf("  query failed: %s\n", r.error.c_str()); return; }
    if (r.rows.empty()) { std::printf("  (no rows)\n"); return; }
    std::vector<size_t> w(r.columns.size());
    for (size_t c = 0; c < w.size(); ++c) w[c] = r.columns[c].size();
    for (const auto& row : r.rows)
        for (size_t c = 0; c < row.size(); ++c) w[c] = std::max(w[c], std::min<size_t>(row[c].size(), 48));
    auto line = [&](const std::vector<std::string>& cells) {
        std::printf("  ");
        for (size_t c = 0; c < cells.size(); ++c)
            std::printf("%-*.*s  ", (int)w[c], 48, cells[c].c_str());
        std::printf("\n");
    };
    line(r.columns);
    std::string sep;
    for (size_t c = 0; c < w.size(); ++c) sep += std::string(w[c], '-') + "  ";
    std::printf("  %s\n", sep.c_str());
    for (const auto& row : r.rows) line(row);
}

void printHelp() {
    std::printf(
        "\nCommands:\n"
        "  submit <TYPE> [scale]   submit one task  (TYPE: WORD_COUNT PRIME_COUNT MATRIX_MULTIPLY CHECKSUM)\n"
        "  burst <N> <TYPE> [scale] submit N tasks back-to-back (shows load spreading)\n"
        "  workers                 live Worker state from PostgreSQL\n"
        "  tasks                   latest tasks from PostgreSQL\n"
        "  history                 execution_history from PostgreSQL\n"
        "  help | quit\n\n");
}

}  // namespace

int main() {
    std::signal(SIGPIPE, SIG_IGN);
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    const int port = envIntOr("NEXUS_MASTER_PORT", proto::DEFAULT_MASTER_PORT);
    const int hb   = envIntOr("NEXUS_HEARTBEAT_SEC", proto::DEFAULT_HEARTBEAT_SEC);

    std::printf("=====================================================\n"
                "  NEXUS MASTER  -  resource-aware distributed system\n"
                "=====================================================\n");

    Database db(Database::Config::fromEnv());
    std::string err;
    if (!db.connect(err)) {
        log("MASTER", "Cannot connect to PostgreSQL: " + err);
        log("MASTER", "Check NEXUS_DB_HOST / PORT / NAME / USER / PASSWORD (see .env.example)");
        return 1;
    }
    log("MASTER", "Connected to PostgreSQL");
    db.markAllWorkersInactive();

    WorkerManager workers(db);
    Scheduler scheduler;
    TaskManager tasks(db, workers, scheduler);
    MasterServer server(port, hb, workers, tasks);
    if (!server.start(err)) {
        log("MASTER", "Cannot listen on port " + std::to_string(port) + ": " + err);
        return 1;
    }
    log("MASTER", "Listening for Workers on TCP port " + std::to_string(port));
    printHelp();

    // Console loop. poll() on stdin so Ctrl+C is noticed promptly.
    std::string line;
    bool stdinOpen = true;
    while (!g_stop) {
        if (!stdinOpen) { std::this_thread::sleep_for(std::chrono::milliseconds(200)); continue; }
        pollfd p;
        p.fd = STDIN_FILENO; p.events = POLLIN; p.revents = 0;
        int pr = ::poll(&p, 1, 300);
        if (pr <= 0) continue;
        if (!std::getline(std::cin, line)) { stdinOpen = false; continue; }  // EOF: keep serving until Ctrl+C

        std::istringstream in(line);
        std::string cmd;
        in >> cmd;
        if (cmd.empty()) continue;

        if (cmd == "quit" || cmd == "exit") {
            break;
        } else if (cmd == "help") {
            printHelp();
        } else if (cmd == "submit") {
            std::string type; int scale = 1;
            in >> type >> scale;
            if (type.empty()) { std::printf("usage: submit <TYPE> [scale]\n"); continue; }
            std::transform(type.begin(), type.end(), type.begin(), ::toupper);
            if (!tasks.submit(type, scale, err)) log("MASTER", "Submit failed: " + err);
        } else if (cmd == "burst") {
            int n = 0, scale = 1; std::string type;
            in >> n >> type >> scale;
            if (n < 1 || type.empty()) { std::printf("usage: burst <N> <TYPE> [scale]\n"); continue; }
            std::transform(type.begin(), type.end(), type.begin(), ::toupper);
            for (int i = 0; i < n; ++i)
                if (!tasks.submit(type, scale, err)) { log("MASTER", "Submit failed: " + err); break; }
        } else if (cmd == "workers") {
            printTable(db.exec(
                "SELECT worker_id, hostname, status, round(cpu_usage::numeric,1) AS cpu_pct, "
                "round(ram_usage::numeric,1) AS ram_pct, round(disk_usage::numeric,1) AS disk_pct, "
                "cpu_cores, memory_total_mb AS mem_mb, to_char(last_heartbeat,'HH24:MI:SS') AS last_hb "
                "FROM workers ORDER BY worker_id"));
        } else if (cmd == "tasks") {
            printTable(db.exec(
                "SELECT task_id, job_id, task_type, assigned_worker AS worker, status, "
                "to_char(completed_at,'HH24:MI:SS') AS done_at, result "
                "FROM tasks ORDER BY task_id DESC LIMIT 15"));
        } else if (cmd == "history") {
            printTable(db.exec(
                "SELECT execution_id AS id, task_id, worker_id, task_type, "
                "round(execution_time::numeric,3) AS exec_s, status, "
                "round(worker_cpu_at_assign::numeric,0) AS cpu_at_assign, scheduler_policy "
                "FROM execution_history ORDER BY execution_id DESC LIMIT 15"));
        } else {
            std::printf("unknown command '%s' (try: help)\n", cmd.c_str());
        }
    }

    log("MASTER", "Shutting down...");
    server.stop();
    return 0;
}
