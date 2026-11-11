# Test harness only: the deterministic simulator never ships.
ADD_LIBRARY(stepraft-sim STATIC
    cRaftSafetyObserver.cpp
    cRaftSimulator.cpp
    cRaftSweep.cpp
    cRaftTraceWriter.cpp
    cSimulationRandom.cpp
)
TARGET_INCLUDE_DIRECTORIES(stepraft-sim PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
TARGET_LINK_LIBRARIES(stepraft-sim PUBLIC stepraft stepraft-warnings)

ADD_EXECUTABLE(stepraft-tests
    cInMemoryRaftLogTest.cpp
    cRaftMembershipTest.cpp
    cRaftNodeElectionTest.cpp
    cRaftNodeReplicationTest.cpp
    cRaftNodeSweepTest.cpp
    cRaftNodeTest.cpp
    cRaftProgressTest.cpp
    cRaftSafetyObserverTest.cpp
    cRaftSimulatorTest.cpp
    cSimulationRandomTest.cpp
)
TARGET_LINK_LIBRARIES(stepraft-tests PRIVATE stepraft-sim GTest::gtest_main)
GTEST_DISCOVER_TESTS(stepraft-tests)

ADD_EXECUTABLE(raft_simulator_bench cRaftSimulatorBench.cpp)
TARGET_LINK_LIBRARIES(raft_simulator_bench PRIVATE stepraft-sim)
ADD_TEST(NAME RaftSimulatorBenchSmoke
    COMMAND raft_simulator_bench --seeds 10 --ticks 200)

# Mass sweeps run by hand on a release build; ctest only checks the tool.
ADD_EXECUTABLE(raft_sweep cRaftSweepTool.cpp)
TARGET_LINK_LIBRARIES(raft_sweep PRIVATE stepraft-sim)
ADD_TEST(NAME RaftSweepSmoke COMMAND raft_sweep --seeds 5 --ticks 500)

# Writes simulator runs as NDJSON traces for tla/Tracestepraft.tla.
ADD_EXECUTABLE(raft_trace cRaftTraceTool.cpp)
TARGET_LINK_LIBRARIES(raft_trace PRIVATE stepraft-sim)
ADD_TEST(NAME RaftTraceValidation
    COMMAND ${PROJECT_SOURCE_DIR}/tla/trace_test.sh $<TARGET_FILE:raft_trace>)
SET_TESTS_PROPERTIES(RaftTraceValidation PROPERTIES
    SKIP_REGULAR_EXPRESSION "^skipped:" TIMEOUT 600)
