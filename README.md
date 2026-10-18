# NEXUS
### AI-Assisted, Resource-Aware Distributed Data Processing and Storage System

NEXUS is a team project that explores distributed computing by coordinating a master server and multiple worker processes. It combines operating-system-level resource monitoring, task scheduling, network communication, and database-backed execution tracking.

The system is inspired by distributed computing frameworks such as Hadoop. Its current prototype focuses on worker coordination, resource-aware task assignment, task execution, and persistent monitoring data.

## Table of Contents

- [Objectives](#objectives)
- [Key Features](#key-features)
- [System Architecture](#system-architecture)
- [Task Scheduling](#task-scheduling)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Database Design](#database-design)
- [Prerequisites](#prerequisites)
- [Setup and Build](#setup-and-build)
- [Running NEXUS](#running-nexus)
- [Available Commands](#available-commands)
- [Current Scope and Future Enhancements](#current-scope-and-future-enhancements)

## Objectives

- Explore how distributed workloads can be coordinated across multiple workers.
- Monitor CPU, RAM, and disk usage to support resource-aware scheduling.
- Use TCP communication for master-worker registration, heartbeats, and task execution.
- Store worker information, task states, resource metrics, and execution history in PostgreSQL.
- Build a foundation for future AI-assisted scheduling and distributed data storage.

## Key Features

- **Master-worker architecture:** A central master coordinates connected worker processes.
- **Worker registration:** Workers register their identifiers, hostnames, CPU core counts, and memory capacity.
- **Resource monitoring:** Workers collect CPU, memory, and disk utilization measurements.
- **Heartbeat monitoring:** Periodic heartbeat messages allow the master to identify inactive or disconnected workers.
- **Resource-aware scheduling:** A scoring-based scheduler selects an active worker using resource utilization and running-task count.
- **Task execution:** Workers process supported computational task types and return results or failure information.
- **Persistent metadata:** PostgreSQL stores jobs, tasks, worker state, resource measurements, and execution records.
- **Execution reporting:** The master console provides commands to inspect workers, recent tasks, and execution history.
- **Concurrent communication:** C++ threads support connection handling and background worker monitoring.

## System Architecture

NEXUS follows a master-worker architecture with four main layers.

```text
             USER / TASK SUBMISSION
                      |
                      v
          +------------------------+
          |      MASTER SERVER     |
          | Registration & Control |
          +------------------------+
             |          |       |
             v          v       v
        +---------+ +---------+ +----------+
        | Worker  | | Worker  | | Scheduler|
        | Manager | | Monitor | | & Tasks  |
        +---------+ +---------+ +----------+
             |          |
             +---- TCP -+
                      |
          +-----------+-----------+
          |                       |
          v                       v
     +----------+            +----------+
     | Worker 1 |            | Worker 2 |
     | Monitor  |            | Executor |
     +----------+            +----------+

             MASTER <----> POSTGRESQL
                 Persistent Metadata
```

### 1. Master Server

The master is the central coordination point. It:

- Accepts worker connections over TCP.
- Validates worker registration and acknowledges successful connections.
- Receives heartbeats, resource updates, and task results.
- Coordinates scheduling and task assignment.
- Tracks worker availability and handles connection timeouts.
- Provides an interactive console for task submission and database-backed reports.

### 2. Worker Manager

The Worker Manager maintains the master's in-memory view of connected workers. It tracks worker identity, connection state, resource measurements, heartbeat timestamps, and running-task counts.

Workers that miss the configured heartbeat timeout are marked inactive and excluded from scheduling.

### 3. Scheduler and Task Manager

The Scheduler evaluates available workers and chooses a suitable execution target. The Task Manager coordinates job creation, task assignment, result processing, failure recording, and execution history.

### 4. Worker Processes

Each worker connects to the master and registers its identity and machine information. It periodically reports resource usage, receives task assignments, executes supported workloads, and sends execution results back to the master.

Multiple worker processes can be launched to demonstrate distributed task assignment.

## Task Scheduling

NEXUS currently uses a weighted resource-load scoring approach rather than an integrated machine-learning model.

The scheduler calculates a load score using:

```text
Load Score =
    0.6 × CPU Usage
  + 0.3 × RAM Usage
  + 0.1 × Disk Usage
  + 10 × Running Task Count
```

The scheduler selects the active worker with the lowest score. CPU utilization has the greatest weight, while RAM, disk usage, and running tasks contribute to the decision.

If no active workers are available, the scheduler cannot assign the task.

This approach demonstrates resource-aware scheduling and provides a foundation for future AI-assisted scheduling policies.

## Technology Stack

| Technology | Purpose |
|---|---|
| C++17 | Core application, scheduling, communication, and execution |
| POSIX sockets / TCP | Communication between master and workers |
| C++ threads | Concurrent connection handling and monitoring |
| PostgreSQL | Persistent metadata and execution history |
| libpq | PostgreSQL connectivity from C++ |
| CMake | Build configuration and executable generation |
| Linux / WSL 2 | Runtime environment for operating-system resource monitoring |

## Project Structure

```text
NEXUS/
├── common/
│   ├── message.cpp
│   ├── message.h
│   ├── protocol.h
│   ├── config.h
│   └── ...
├── database/
│   ├── database.cpp
│   ├── database.h
│   ├── schema.sql
│   └── seed.sql
├── master/
│   ├── main.cpp
│   ├── master_server.cpp
│   ├── master_server.h
│   ├── worker_manager.cpp
│   ├── scheduler.cpp
│   ├── task_manager.cpp
│   └── ...
├── worker/
│   ├── main.cpp
│   ├── worker_client.cpp
│   ├── resource_monitor.cpp
│   └── ...
├── .env.example
├── .gitignore
└── CMakeLists.txt
```

- **common/** — Shared message handling, communication protocol, configuration, and TCP utilities.
- **database/** — PostgreSQL connection and data-access logic, schema definitions, and seed data.
- **master/** — Server lifecycle, worker management, scheduling, task coordination, and console interface.
- **worker/** — Worker connection logic, resource monitoring, and task execution.
- **CMakeLists.txt** — Defines the common library, database library, and master and worker executables.
- **.env.example** — Documents the environment variables required to configure the application.

## Database Design

PostgreSQL acts as the persistent metadata store. Workers do not access the database directly; database operations are performed by the master.

| Table | Purpose |
|---|---|
| `workers` | Worker identity, machine information, status, and latest resource readings |
| `jobs` | Job type, lifecycle status, and submission/completion timestamps |
| `tasks` | Task parameters, assigned worker, status, and result |
| `resource_metrics` | Timestamped worker resource measurements |
| `execution_history` | Execution duration, outcome, worker, and scheduling context |

The schema also defines indexes for common worker, task, metric, and execution-history queries.

## Prerequisites

The current code uses Linux/POSIX interfaces, so Linux or WSL 2 with a Linux distribution is the recommended environment.

Install the required packages on Ubuntu or Debian:

```bash
sudo apt update
sudo apt install build-essential cmake libpq-dev postgresql
```

Ensure that the PostgreSQL service is running before launching the master.

## Setup and Build

### 1. Open the project in Linux / WSL

Navigate to the directory containing the repository. If it is stored on your Windows D: drive, a typical WSL path is:

```bash
cd "/mnt/d/Documents/for coding/OS-DBMS Project Nexus/NEXUS"
```

Adjust the path if your project is stored elsewhere.

### 2. Create the database and user

Start PostgreSQL:

```bash
sudo service postgresql start
```

Open the PostgreSQL shell:

```bash
sudo -u postgres psql
```

Create a dedicated user and database, using a password you choose:

```sql
CREATE USER nexus WITH PASSWORD 'choose_a_password';
CREATE DATABASE nexus OWNER nexus;
\q
```

### 3. Configure the environment

Copy the example configuration:

```bash
cp .env.example .env
```

Edit `.env` and set the PostgreSQL password and other connection values to match your local setup. Do not commit `.env` or real credentials.

Load the configuration into your current shell:

```bash
set -a
source .env
set +a
```

### 4. Initialize the schema

Run the database schema script:

```bash
psql -h "$NEXUS_DB_HOST" -p "$NEXUS_DB_PORT" \
  -U "$NEXUS_DB_USER" -d "$NEXUS_DB_NAME" \
  -f database/schema.sql
```

Enter the configured database password when prompted.

### 5. Build the project

From the repository root:

```bash
cmake -S . -B build
cmake --build build
```

The compiled executables will be located in the `build/` directory.

## Running NEXUS

Run the master and workers in separate Linux/WSL terminals.

### Terminal 1: Start the master

Load the environment configuration in this terminal and run:

```bash
./build/master
```

The master connects to PostgreSQL and listens for worker connections on the configured TCP port, which defaults to `5050`.

### Terminal 2: Start the first worker

Load the same relevant environment configuration and run:

```bash
./build/worker worker-1
```

### Terminal 3: Start another worker

```bash
./build/worker worker-2
```

Use unique worker IDs for each worker. For workers on different machines, configure `NEXUS_MASTER_HOST` to the master's reachable network address and ensure the TCP port is accessible.

Once workers register successfully, use the master console to submit tasks and inspect execution results.

## Available Commands

Enter these commands in the running master's interactive console.

| Command | Description |
|---|---|
| `help` | Display available commands |
| `workers` | Show worker status and resource readings |
| `tasks` | Show recent tasks, status, and results |
| `history` | Show recent execution records |
| `submit WORD_COUNT 10` | Submit a task with the specified scale |
| `submit PRIME_COUNT 10` | Submit a prime-counting workload |
| `submit MATRIX_MULTIPLY 10` | Submit a matrix multiplication workload |
| `submit CHECKSUM 10` | Submit a checksum workload |
| `burst 5 CHECKSUM 10` | Submit five tasks in succession |
| `quit` | Shut down the master gracefully |

The scale parameter controls the workload size supported by the task implementation. Exact execution time depends on the workload and machine.

## Current Scope and Future Enhancements

NEXUS is being developed incrementally. The current prototype focuses on master-worker coordination, resource monitoring, scheduling, task execution, and PostgreSQL-backed metadata.

Potential future enhancements include:

- **AI-assisted scheduling:** Train and integrate a model to estimate execution time or recommend worker assignments using historical execution data.
- **Distributed storage:** Implement file chunking, worker-side storage, replication, and retrieval inspired by HDFS.
- **Fault recovery:** Reschedule tasks after worker failures instead of only recording them as failed.
- **Monitoring and visualization:** Add a dashboard for worker health, resource utilization, task states, and execution history.
- **Expanded testing:** Validate scheduling decisions, concurrent workers, network failures, and database error handling.

These are planned extensions, not claims that every feature is already implemented.

## Academic Context

NEXUS is a team-based Operating Systems and Database Management Systems project. It combines concepts including process and thread coordination, resource monitoring, network communication, task scheduling, relational database design, and execution-state tracking.

The goal is to develop a practical, understandable prototype of a resource-aware distributed computing platform and extend it incrementally.
