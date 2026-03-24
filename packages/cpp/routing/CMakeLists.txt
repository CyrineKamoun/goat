cmake_minimum_required(VERSION 3.18)
project(routing LANGUAGES CXX)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)

# --- DuckDB (pre-built release binary) ---
set(DUCKDB_VERSION "v1.4.4")

# Detect platform for pre-built binary
if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64|ARM64")
    set(DUCKDB_PLATFORM "linux-arm64")
else()
    set(DUCKDB_PLATFORM "linux-amd64")
endif()

FetchContent_Declare(
    duckdb
    URL https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/libduckdb-${DUCKDB_PLATFORM}.zip
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
FetchContent_MakeAvailable(duckdb)

add_library(duckdb SHARED IMPORTED GLOBAL)
set_target_properties(duckdb PROPERTIES
    IMPORTED_LOCATION "${duckdb_SOURCE_DIR}/libduckdb.so"
    INTERFACE_INCLUDE_DIRECTORIES "${duckdb_SOURCE_DIR}"
)
# Install libduckdb.so alongside our module so it can be found at runtime
install(FILES "${duckdb_SOURCE_DIR}/libduckdb.so" DESTINATION routing)

# --- pybind11 ---
find_package(pybind11 CONFIG REQUIRED)

file(GLOB_RECURSE SOURCES
    src/input/*.cpp
    src/data/*.cpp
    src/kernel/*.cpp
    src/pt/*.cpp
    src/geometry/*.cpp
    src/output/*.cpp
    src/pipeline.cpp
)

pybind11_add_module(_routing
    src/pybind_module.cpp
    ${SOURCES}
)

target_include_directories(_routing PRIVATE src)
target_link_libraries(_routing PRIVATE duckdb)

install(TARGETS _routing LIBRARY DESTINATION routing)
