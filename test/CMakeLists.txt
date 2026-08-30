cmake_minimum_required(VERSION 3.10)

include(GoogleTest)

file(GLOB_RECURSE BUSTUB_TEST_SOURCES "${PROJECT_SOURCE_DIR}/test/*/*test.cpp")

# #####################################################################################################################
# MAKE TARGETS
# #####################################################################################################################

# #########################################
# "make check-tests"
# #########################################
add_custom_target(build-tests COMMAND ${CMAKE_CTEST_COMMAND} --show-only)
add_custom_target(check-tests COMMAND ${CMAKE_CTEST_COMMAND} --verbose -LE raft-process-chain)
add_custom_target(check-public-ci-tests COMMAND ${CMAKE_CTEST_COMMAND} --verbose -LE raft-process-chain -E "\"SQLLogicTest|Trie|ORSet|BPlusTreeContentionTest\"")

# #########################################
# "make XYZ_test"
# #########################################
foreach (bustub_test_source ${BUSTUB_TEST_SOURCES})
    # Create a human readable name.
    get_filename_component(bustub_test_filename ${bustub_test_source} NAME)
    string(REPLACE ".cpp" "" bustub_test_name ${bustub_test_filename})

    # Add the test target separately and as part of "make check-tests".
    add_executable(${bustub_test_name} EXCLUDE_FROM_ALL ${bustub_test_source} "${PROJECT_SOURCE_DIR}/tools/backtrace.cpp")
    target_include_directories(${bustub_test_name} PRIVATE "${BUSTUB_TEST_INCLUDE_DIR}")
    add_backward(${bustub_test_name})
    add_dependencies(build-tests ${bustub_test_name})
    add_dependencies(check-tests ${bustub_test_name})

    set(bustub_test_labels "unit")
    if (bustub_test_source MATCHES "/test/(recovery|raft|distributed)/")
        list(APPEND bustub_test_labels "raft")
    endif ()
    if (bustub_test_name STREQUAL "tcp_transport_test" OR bustub_test_name STREQUAL "distributed_node_test")
        list(APPEND bustub_test_labels "distributed-e2e")
    endif ()
    if (bustub_test_name STREQUAL "tcp_transport_test" OR
            bustub_test_name STREQUAL "raft_node_test" OR
            bustub_test_name STREQUAL "bustub_state_machine_test" OR
            bustub_test_name STREQUAL "distributed_node_test")
        list(APPEND bustub_test_labels "raft-tsan-core")
    endif ()

    # gtest_discover_tests forwards PROPERTIES through a generated CMake
    # script. A semicolon-valued LABELS list is flattened there into stray
    # property names. One composite label preserves every searchable facet:
    # CTest -L/-LE use regex matching, so `-L raft-tsan-core`, `-L raft`, and
    # `-L distributed-e2e` still select exactly the intended tests.
    string(REPLACE ";" "+" bustub_test_label "${bustub_test_labels}")

    gtest_discover_tests(${bustub_test_name}
            EXTRA_ARGS
            --gtest_output=xml:${CMAKE_BINARY_DIR}/test/${bustub_test_name}.xml
            --gtest_catch_exceptions=0
            DISCOVERY_TIMEOUT 120
            DISCOVERY_MODE PRE_TEST
            PROPERTIES
            TIMEOUT 120
            LABELS "${bustub_test_label}"
            )

    target_link_libraries(${bustub_test_name} bustub gtest gmock_main)

    # Set test target properties and dependencies.
    set_target_properties(${bustub_test_name}
            PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/test"
            COMMAND ${bustub_test_name}
            )
endforeach ()

# Keep the process-strict M0-M7 component set buildable from a clean tree with
# one documented target. raft_gtest_gate.py independently reverse-scans the
# corresponding source set and rejects a stale execution manifest.
set(RAFT_COMPONENT_GATE_TARGETS
        versioned_frame_test
        state_manifest_test
        table_heap_reopen_test
        canonical_snapshot_test
        log_codec_test
        snapshot_manager_test
        power_loss_storage_test
        single_node_runtime_test
        command_log_test
        stable_store_test
        tcp_transport_test
        client_protocol_test
        node_config_test
        command_codec_test
        session_table_test
        rpc_codec_test
        raft_state_machine_test
        sql_command_preparer_test
        raft_node_test
        bustub_state_machine_test
        catalog_snapshot_test
        log_store_test
        raft_bustub_cluster_test
        snapshot_store_test
        read_timestamp_test
        distributed_node_test)
add_custom_target(build-raft-component-gates)
add_dependencies(build-raft-component-gates ${RAFT_COMPONENT_GATE_TARGETS})

# Register every checked-in SQLLogic fixture. Keep the order deterministic so
# local CTest output and the dedicated Release CI gate are directly comparable.
file(GLOB BUSTUB_SLT_SOURCES "${PROJECT_SOURCE_DIR}/test/sql/*.slt")
list(SORT BUSTUB_SLT_SOURCES)

add_custom_target(test-p3 ${CMAKE_CTEST_COMMAND} -R SQLLogicTest)

# #########################################
# "make XYZ_test"
# #########################################
foreach (bustub_test_source ${BUSTUB_SLT_SOURCES})
    # Create a human readable name.
    get_filename_component(bustub_test_filename ${bustub_test_source} NAME)
    string(REPLACE ".slt" "" bustub_filename_wo_suffix "${bustub_test_filename}")
    string(REPLACE ".slt" "" bustub_test_name "SQLLogicTest.${bustub_filename_wo_suffix}")
    add_test(NAME ${bustub_test_name} COMMAND "${CMAKE_BINARY_DIR}/bin/bustub-sqllogictest" ${bustub_test_source} --verbose -d --in-memory)
    # Leaderboard fixtures can legitimately exceed five minutes when two Release
    # cases share a small CI runner. Keep the per-test timeout below the job
    # budget while avoiding a resource-contention false negative.
    set_tests_properties(${bustub_test_name} PROPERTIES LABELS "sqllogic" TIMEOUT 900)
    add_custom_target(${bustub_filename_wo_suffix}_test COMMAND "${CMAKE_BINARY_DIR}/bin/bustub-sqllogictest" "${bustub_test_source}" --verbose -d --in-memory)
    add_dependencies(${bustub_filename_wo_suffix}_test sqllogictest)
endforeach ()

add_dependencies(test-p3 sqllogictest)

# Must build sqllogictest before checking tests
add_dependencies(build-tests sqllogictest)
add_dependencies(check-tests sqllogictest)

# This is deliberately separate from the ordinary unit/SQLLogicTest targets: it
# owns real loopback ports, starts three production node processes, and carries
# one fresh distributed durable state through the M3-M7 assembly while cumulatively
# rechecking earlier logical properties. It does not migrate term-0 files. The script
# creates a fresh artifact root for every CTest process and rejects reuse.
add_test(
        NAME RaftProcessChain
        COMMAND bash "${PROJECT_SOURCE_DIR}/test/e2e/raft_m0_m7_chain.sh" "${CMAKE_BINARY_DIR}" 24100)
set_tests_properties(
        RaftProcessChain
        PROPERTIES
        LABELS "raft;distributed-e2e;raft-process-chain"
        RUN_SERIAL TRUE
        TIMEOUT 900)
add_custom_target(
        check-raft-process-chain
        COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -L raft-process-chain)
add_dependencies(check-raft-process-chain bustub-node bustub-client)
