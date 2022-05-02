cmake_minimum_required(VERSION 3.10)
project(RockRaft CXX)

# option(EXAMPLE_LINK_SO "Whether examples are linked dynamically" OFF)
option(LINK_TCMALLOC "Link tcmalloc if possible" ON)

message(STATUS "CXX compiler: ${CMAKE_CXX_COMPILER}, version: "
        "${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "C compiler: ${CMAKE_C_COMPILER}, version: "
        "${CMAKE_C_COMPILER_ID} ${CMAKE_C_COMPILER_VERSION}")

# thread
include(FindThreads)

# protobuf
include(FindProtobuf)

if (NOT PROTOBUF_PROTOC_EXECUTABLE)
    get_filename_component(PROTO_LIB_DIR ${PROTOBUF_LIBRARY} DIRECTORY)
    set (PROTOBUF_PROTOC_EXECUTABLE "${PROTO_LIB_DIR}/../bin/protoc")
endif()

protobuf_generate_cpp(PROTO_SRC PROTO_HEADER proto/rockraft.proto)
# include PROTO_HEADER
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# bz2
find_library(BZ2_LIB NAMES bz2)
if(NOT BZ2_LIB)
    message(FATAL_ERROR "Fail to find bz2")
endif()

# gflags
find_path(GFLAGS_INCLUDE_PATH gflags/gflags.h)
find_library(GFLAGS_LIBRARY NAMES gflags libgflags)
if((NOT GFLAGS_INCLUDE_PATH) OR (NOT GFLAGS_LIBRARY))
    message(FATAL_ERROR "Fail to find gflags")
endif()
include_directories(${GFLAGS_INCLUDE_PATH})

execute_process(
        COMMAND bash -c "grep \"namespace [_A-Za-z0-9]\\+ {\" ${GFLAGS_INCLUDE_PATH}/gflags/gflags_declare.h | head -1 | awk '{print $2}' | tr -d '\n'"
        OUTPUT_VARIABLE GFLAGS_NS
)
if(${GFLAGS_NS} STREQUAL "GFLAGS_NAMESPACE")
    execute_process(
            COMMAND bash -c "grep \"#define GFLAGS_NAMESPACE [_A-Za-z0-9]\\+\" ${GFLAGS_INCLUDE_PATH}/gflags/gflags_declare.h | head -1 | awk '{print $3}' | tr -d '\n'"
            OUTPUT_VARIABLE GFLAGS_NS
    )
endif()

# leveldb
find_path(LEVELDB_INCLUDE_PATH NAMES leveldb/db.h)
find_library(LEVELDB_LIB NAMES leveldb)
if ((NOT LEVELDB_INCLUDE_PATH) OR (NOT LEVELDB_LIB))
    message(FATAL_ERROR "Fail to find leveldb")
endif()
include_directories(${LEVELDB_INCLUDE_PATH})

# lz4
find_library(LZ4_LIB NAMES lz4)
if ((NOT LZ4_LIB))
    message(FATAL_ERROR "Fail to find lz4")
endif()

# tcmalloc
if (LINK_TCMALLOC)
    find_path(GPERFTOOLS_INCLUDE_DIR NAMES gperftools/heap-profiler.h)
    find_library(GPERFTOOLS_LIBRARIES NAMES tcmalloc_and_profiler)
    if (GPERFTOOLS_INCLUDE_DIR AND GPERFTOOLS_LIBRARIES)
        set(CMAKE_CXX_FLAGS "-DBRPC_ENABLE_CPU_PROFILER")
        include_directories(${GPERFTOOLS_INCLUDE_DIR})
    else ()
        set (GPERFTOOLS_LIBRARIES "")
    endif ()
endif ()

# brpc
find_path(BRPC_INCLUDE_PATH NAMES brpc/server.h)
find_library(BRPC_LIB NAMES libbrpc.a brpc)
if((NOT BRPC_INCLUDE_PATH) OR (NOT BRPC_LIB))
    message(FATAL_ERROR "Fail to find brpc")
endif()
include_directories(${BRPC_INCLUDE_PATH})

# braft
find_path(BRAFT_INCLUDE_PATH NAMES braft/raft.h)
find_library(BRAFT_LIB NAMES libbraft.a braft)
if ((NOT BRAFT_INCLUDE_PATH) OR (NOT BRAFT_LIB))
   message (FATAL_ERROR "Fail to find braft")
endif()
include_directories(${BRAFT_INCLUDE_PATH})

# snappy
find_library(SNAPPY_LIB NAMES snappy)
if (NOT SNAPPY_LIB)
    message(FATAL_ERROR "Fail to find snappy")
endif()
ADD_LIBRARY(snappy STATIC IMPORTED GLOBAL)

# zstd
find_library(ZSTD_LIB NAMES zstd)
if (NOT ZSTD_LIB)
    message(FATAL_ERROR "Fail to find zstd")
endif()

# rocksdb
find_path(ROCKSDB_INCLUDE_PATH NAMES rocksdb/db.h)
find_library(ROCKSDB_LIB NAMES rocksdb)
if ((NOT ROCKSDB_INCLUDE_PATH) OR (NOT ROCKSDB_LIB))
   message(FATAL_ERROR "Fail to find rocksdb")
endif()
include_directories(${ROCKSDB_INCLUDE_PATH})


set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CPP_FLAGS} -DGFLAGS_NS=${GFLAGS_NS} -DNDEBUG -O2 -D__const__= -pipe -W -Wall -Wno-unused-parameter -fPIC -fno-omit-frame-pointer")
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # require at least gcc 4.8
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.8)
        message(FATAL_ERROR "GCC is too old, please install a newer version supporting C++17")
    endif()
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # require at least clang 3.3
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 3.3)
        message(FATAL_ERROR "Clang is too old, please install a newer version supporting C++17")
    endif()
else()
    message(WARNING "You are using an unsupported compiler! Compilation has only been tested with Clang and GCC.")
endif()

if(CMAKE_VERSION VERSION_LESS "3.1.3")
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
    endif()
    if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
    endif()
else()
    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
endif()

add_executable(rockraft_client src/rockraft_client.cc ${PROTO_SRC} ${PROTO_HEADER})
add_executable(rockraft_server src/rockraft.cc ${PROTO_SRC} ${PROTO_HEADER})

set(DYNAMIC_LIB
        ${CMAKE_THREAD_LIBS_INIT}
        ${GFLAGS_LIBRARY}
        ${PROTOBUF_LIBRARY}
        ${GPERFTOOLS_LIBRARIES}
        ${LEVELDB_LIB}
        ${ROCKSDB_LIB}
        ${SNAPPY_LIB}
        ${ZSTD_LIB}
        ${LZ4_LIB}
        ${BZ2_LIB}
        ${BRAFT_LIB}
        ${BRPC_LIB}
        rt
        ssl
        crypto
        dl
        z
        )

target_link_libraries(rockraft_client
       "-Xlinker \"-(\""
       ${DYNAMIC_LIB}
       "-Xlinker \"-)\"")
target_link_libraries(rockraft_server
        "-Xlinker \"-(\""
        ${DYNAMIC_LIB}
        "-Xlinker \"-)\"")
