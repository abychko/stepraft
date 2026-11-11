CMAKE_MINIMUM_REQUIRED(VERSION 3.20)

FILE(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/VERSION" STEPRAFT_VERSION_LINES)
FOREACH(PART MAJOR MINOR PATCH)
    FOREACH(LINE IN LISTS STEPRAFT_VERSION_LINES)
        IF(LINE MATCHES "^STEPRAFT_${PART} = ([0-9]+)$")
            SET(STEPRAFT_${PART} "${CMAKE_MATCH_1}")
        ENDIF()
    ENDFOREACH()
ENDFOREACH()
PROJECT(stepraft
    VERSION "${STEPRAFT_MAJOR}.${STEPRAFT_MINOR}.${STEPRAFT_PATCH}"
    LANGUAGES CXX)

OPTION(STEPRAFT_BUILD_TESTS
    "Build the tests, the simulator and its tools" ${PROJECT_IS_TOP_LEVEL})
OPTION(STEPRAFT_STRICT
    "Treat warnings as errors and check each header on its own"
    ${PROJECT_IS_TOP_LEVEL})

ADD_LIBRARY(stepraft-warnings INTERFACE)
IF(STEPRAFT_STRICT)
    TARGET_COMPILE_OPTIONS(stepraft-warnings INTERFACE
        -Wall -Wextra -Wpedantic -Werror -Wmissing-declarations
        -pedantic-errors)
ENDIF()

ADD_LIBRARY(stepraft STATIC
    src/cInMemoryRaftLog.cpp
    src/cRaftMembership.cpp
    src/cRaftNode.cpp
    src/cRaftProgress.cpp
)
ADD_LIBRARY(stepraft::stepraft ALIAS stepraft)
TARGET_COMPILE_FEATURES(stepraft PUBLIC cxx_std_20)
SET_TARGET_PROPERTIES(stepraft PROPERTIES CXX_EXTENSIONS OFF)
TARGET_INCLUDE_DIRECTORIES(stepraft PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/include")
TARGET_LINK_LIBRARIES(stepraft PRIVATE $<BUILD_INTERFACE:stepraft-warnings>)

IF(STEPRAFT_STRICT)
    INCLUDE(cmake/header_check.cmake)
    STEPRAFT_ADD_HEADER_CHECK(stepraft-hc
        "${CMAKE_CURRENT_SOURCE_DIR}/include/stepraft"
        "${CMAKE_CURRENT_SOURCE_DIR}/include")
ENDIF()

IF(STEPRAFT_BUILD_TESTS)
    ENABLE_TESTING()
    INCLUDE(GoogleTest)
    FIND_PACKAGE(GTest REQUIRED)
    ADD_SUBDIRECTORY(tests)
ENDIF()
