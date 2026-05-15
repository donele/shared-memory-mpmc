cmake_minimum_required(VERSION 3.20)
project(shm_cpp_subset LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include_directories(
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}/core"
    "${CMAKE_CURRENT_SOURCE_DIR}/core/types"
)

add_executable(shm_manager
    bin/shm_manager.cpp
)

add_executable(shm_test_producer
    bin/shm_test_producer.cpp
)

add_executable(shm_test_consumer_batch
    bin/shm_test_consumer_batch.cpp
    core/types/time_types.cpp
)
