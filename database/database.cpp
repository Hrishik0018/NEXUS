#include "database.h"

#include <libpq-fe.h>

#include "../common/config.h"
#include "../common/log.h"

namespace nexus {

namespace {

// libpq conninfo values must escape ' and \ and be quoted.
std::string quoteConnValue(const std::string& v) {
    std::string out = "'";
    for (char c : v) {
        if (c == '\'' || c == '\\') out += '\\';
        out += c;
    }
    return out + "'";
}

std::string num(double v) {
    char buf[64];
    std::snprintf(buf, sizeof buf, "%.4f", v);
    return buf;
}

}  // namespace

Database::Config Database::Config::fromEnv() {
    Config c;
    c.host     = envOr("NEXUS_DB_HOST", "localhost");
    c.port     = envOr("NEXUS_DB_PORT", "5432");
    c.name     = envOr("NEXUS_DB_NAME", "nexus");
    c.user     = envOr("NEXUS_DB_USER", "nexus");
    c.password = envOr("NEXUS_DB_PASSWORD", "");   // never hard-coded
    return c;
}

Database::~Database() {
    if (conn_) PQfinish(conn_);
}

bool Database::connect(std::string& err) {
    std::lock_guard<std::mutex> lock(mutex_);
    return ensureConnected(err);
}

bool Database::ensureConnected(std::string& err) {
    if (conn_ && PQstatus(conn_) == CONNECTION_OK) return true;

    if (conn_) {
        PQreset(conn_);                       // connection dropped: try to revive
    } else {
        std::string ci = "host=" + quoteConnValue(cfg_.host) +
                         " port=" + quoteConnValue(cfg_.port) +
                         " dbname=" + quoteConnValue(cfg_.name) +
                         " user=" + quoteConnValue(cfg_.user) +
                         " connect_timeout=5";
        if (!cfg_.password.empty()) ci += " password=" + quoteConnValue(cfg_.password);
        conn_ = PQconnectdb(ci.c_str());
    }
    if (!conn_ || PQstatus(conn_) != CONNECTION_OK) {
        err = conn_ ? PQerrorMessage(conn_) : "PQconnectdb returned null";
        return false;
    }
    return true;
}

Database::Result Database::exec(const std::string& sql, const std::vector<std::string>& params) {
    Result r;
    std::lock_guard<std::mutex> lock(mutex_);
    if (!ensureConnected(r.error)) return r;

    std::vector<const char*> values;
    for (const auto& p : params) values.push_back(p.c_str());

    PGresult* res = PQexecParams(conn_, sql.c_str(), static_cast<int>(values.size()),
                                 nullptr, values.empty() ? nullptr : values.data(),
                                 nullptr, nullptr, 0);
    ExecStatusType st = PQresultStatus(res);
    if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
        r.error = PQerrorMessage(conn_);
        log("DB", "Query failed: " + r.error);
        PQclear(res);
        return r;
    }
    int nf = PQnfields(res), nr = PQntuples(res);
    for (int c = 0; c < nf; ++c) r.columns.push_back(PQfname(res, c));
    for (int i = 0; i < nr; ++i) {
        std::vector<std::string> row;
        for (int c = 0; c < nf; ++c) row.push_back(PQgetisnull(res, i, c) ? "" : PQgetvalue(res, i, c));
        r.rows.push_back(std::move(row));
    }
    PQclear(res);
    r.ok = true;
    return r;
}

// ------------------------------------------------------------- Workers ----

bool Database::markAllWorkersInactive() {
    // On Master start no Worker is connected yet; clear stale ACTIVE flags.
    return exec("UPDATE workers SET status='INACTIVE'").ok;
}

bool Database::upsertWorker(const std::string& id, const std::string& hostname,
                            const std::string& ip, int cpuCores, long long memMb) {
    return exec(
        "INSERT INTO workers (worker_id, hostname, ip_address, cpu_cores, memory_total_mb, "
        "                     status, last_heartbeat) "
        "VALUES ($1, $2, $3, $4::int, $5::bigint, 'ACTIVE', now()) "
        "ON CONFLICT (worker_id) DO UPDATE SET "
        "  hostname=EXCLUDED.hostname, ip_address=EXCLUDED.ip_address, "
        "  cpu_cores=EXCLUDED.cpu_cores, memory_total_mb=EXCLUDED.memory_total_mb, "
        "  status='ACTIVE', last_heartbeat=now()",
        {id, hostname, ip, std::to_string(cpuCores), std::to_string(memMb)}).ok;
}

bool Database::recordHeartbeat(const std::string& id) {
    return exec("UPDATE workers SET status='ACTIVE', last_heartbeat=now() WHERE worker_id=$1",
                {id}).ok;
}

bool Database::recordResources(const std::string& id, double cpu, double ram, double disk) {
    // One statement: update the live row, then log the sample in resource_metrics.
    return exec(
        "WITH u AS ("
        "  UPDATE workers SET cpu_usage=$2::double precision, ram_usage=$3::double precision, "
        "         disk_usage=$4::double precision, status='ACTIVE', last_heartbeat=now() "
        "  WHERE worker_id=$1 RETURNING worker_id) "
        "INSERT INTO resource_metrics (worker_id, cpu_usage, ram_usage, disk_usage, status) "
        "SELECT worker_id, $2::double precision, $3::double precision, $4::double precision, 'ACTIVE' FROM u",
        {id, num(cpu), num(ram), num(disk)}).ok;
}

bool Database::setWorkerStatus(const std::string& id, const std::string& status) {
    return exec("UPDATE workers SET status=$2 WHERE worker_id=$1", {id, status}).ok;
}

// --------------------------------------------------------- Jobs / tasks ----

bool Database::createJobAndTask(const std::string& type, const std::string& params,
                                long long& jobId, long long& taskId, std::string& err) {
    Result j = exec("INSERT INTO jobs (job_type) VALUES ($1) RETURNING job_id", {type});
    if (!j.ok || j.rows.empty()) { err = j.error; return false; }
    jobId = std::stoll(j.rows[0][0]);

    Result t = exec("INSERT INTO tasks (job_id, task_type, params) VALUES ($1::int, $2, $3) "
                    "RETURNING task_id", {std::to_string(jobId), type, params});
    if (!t.ok || t.rows.empty()) { err = t.error; return false; }
    taskId = std::stoll(t.rows[0][0]);
    return true;
}

bool Database::markTaskAssigned(long long taskId, const std::string& workerId) {
    return exec("UPDATE tasks SET status='ASSIGNED', assigned_worker=$2, assigned_at=now() "
                "WHERE task_id=$1::int", {std::to_string(taskId), workerId}).ok;
}

bool Database::markTaskStarted(long long taskId) {
    bool a = exec("UPDATE tasks SET status='RUNNING', started_at=now() WHERE task_id=$1::int",
                  {std::to_string(taskId)}).ok;
    bool b = exec("UPDATE jobs SET status='RUNNING' WHERE job_id=(SELECT job_id FROM tasks WHERE task_id=$1::int)",
                  {std::to_string(taskId)}).ok;
    return a && b;
}

bool Database::markTaskCompleted(long long taskId, const std::string& result) {
    bool a = exec("UPDATE tasks SET status='COMPLETED', completed_at=now(), result=$2 "
                  "WHERE task_id=$1::int", {std::to_string(taskId), result}).ok;
    bool b = exec("UPDATE jobs SET status='COMPLETED', completed_at=now() "
                  "WHERE job_id=(SELECT job_id FROM tasks WHERE task_id=$1::int)",
                  {std::to_string(taskId)}).ok;
    return a && b;
}

bool Database::markTaskFailed(long long taskId, const std::string& reason) {
    bool a = exec("UPDATE tasks SET status='FAILED', completed_at=now(), result=$2 "
                  "WHERE task_id=$1::int", {std::to_string(taskId), reason}).ok;
    bool b = exec("UPDATE jobs SET status='FAILED', completed_at=now() "
                  "WHERE job_id=(SELECT job_id FROM tasks WHERE task_id=$1::int)",
                  {std::to_string(taskId)}).ok;
    return a && b;
}

bool Database::recordExecution(long long taskId, const std::string& workerId,
                               const std::string& taskType, double execSeconds,
                               const std::string& status, double cpuAtAssign,
                               double ramAtAssign, const std::string& policy) {
    return exec(
        "INSERT INTO execution_history (task_id, worker_id, task_type, execution_time, status, "
        "  worker_cpu_at_assign, worker_ram_at_assign, scheduler_policy) "
        "VALUES ($1::int, $2, $3, $4::double precision, $5, $6::double precision, "
        "        $7::double precision, $8)",
        {std::to_string(taskId), workerId, taskType, num(execSeconds), status,
         num(cpuAtAssign), num(ramAtAssign), policy}).ok;
}

}  // namespace nexus
