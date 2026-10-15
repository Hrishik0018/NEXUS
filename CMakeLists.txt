cmake_minimum_required(VERSION 3.10)
project(NEXUS VERSION 0.4 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)      # task workloads are meant to be optimised
endif()
add_compile_options(-Wall -Wextra)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})   # build/master, build/worker

find_package(Threads REQUIRED)
find_package(PostgreSQL REQUIRED)    # libpq (Debian/Ubuntu: libpq-dev)

# ---- shared: protocol + message framing + TCP helpers -----------------------
add_library(nexus_common STATIC common/message.cpp)
target_link_libraries(nexus_common PUBLIC Threads::Threads)

# ---- PostgreSQL data-access layer --------------------------------------------
add_library(nexus_db STATIC database/database.cpp)
target_include_directories(nexus_db PUBLIC ${PostgreSQL_INCLUDE_DIRS})
target_link_libraries(nexus_db PUBLIC ${PostgreSQL_LIBRARIES} Threads::Threads)

# ---- Master --------------------------------------------------------------------
add_executable(master
  master/main.cpp master/master_server.cpp master/worker_manager.cpp
  master/scheduler.cpp master/task_manager.cpp)
target_link_libraries(master nexus_common nexus_db)

# ---- Worker (no database access: only the Master talks to PostgreSQL) ----------
add_executable(worker
  worker/main.cpp worker/worker_client.cpp worker/resource_monitor.cpp)
target_link_libraries(worker nexus_common)
