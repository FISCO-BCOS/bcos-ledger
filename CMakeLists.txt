#------------------------------------------------------------------------------
# Top-level CMake file for bcos-ledger
# ------------------------------------------------------------------------------
# Copyright (C) 2021 FISCO BCOS.
# SPDX-License-Identifier: Apache-2.0
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ------------------------------------------------------------------------------

cmake_minimum_required(VERSION 3.10)
set(CMAKE_OSX_DEPLOYMENT_TARGET "10.13" CACHE STRING "Minimum OS X deployment version")

include(FetchContent)
FetchContent_Declare(bcos-cmake-scripts
        GIT_REPOSITORY https://github.com/FISCO-BCOS/bcos-cmake-scripts.git
        GIT_TAG dev
        SOURCE_DIR bcos-cmake-scripts
        )
FetchContent_MakeAvailable(bcos-cmake-scripts)

# cmake settings
set(BCOS_CMAKE_SCRIPTS "${CMAKE_BINARY_DIR}/bcos-cmake-scripts")
set(BCOS_CMAKE_SCRIPTS_DIR "${CMAKE_BINARY_DIR}/bcos-cmake-scripts")
set(BCOS_LEDGER_CMAKE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/cmake" CACHE PATH "The cmake path for the library")
list(APPEND CMAKE_MODULE_PATH ${BCOS_CMAKE_SCRIPTS_DIR})
list(APPEND CMAKE_MODULE_PATH ${BCOS_LEDGER_CMAKE_DIR})

# init hunter
include(HunterGate)
HunterGate(
        URL "https://github.com/FISCO-BCOS/hunter/archive/4ba91e1dc10bd74cb757be4ea40d2b3657d808ac.tar.gz"
        SHA1 "9d92a895063ce5f174593686c5f0b0f52377e39d"
)

project(bcos-ledger VERSION "3.0")
# Suffix like "-rc1" e.t.c. to append to versions wherever needed.
set(VERSION_SUFFIX "")

# basic setting
include(Options)
configure_project()
include(CompilerSettings)

# install headers firstly
include(CopyInclude)

# install dependencies
include(InstallBcosFrameworkDependencies)
# install tbb dependency
#hunter_add_package(tbb)

include_directories(${CMAKE_INSTALL_INCLUDEDIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR}/include)
set(INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/include)

# set library target name
set(BCOS_LEDGER_TARGET "bcos-ledger")
set(BCOS_EVENT_FILTER_TARGET "bcos-event-filter")

# add subdirectories to build the libraries
add_subdirectory(ledger)
add_subdirectory(eventfilter)

# test related
if (TESTS)
    hunter_add_package(Boost COMPONENTS test)
    enable_testing()
    set(CTEST_OUTPUT_ON_FAILURE TRUE)
    add_subdirectory(test)
endif()

include(InstallConfig)
install(
        TARGETS ${BCOS_LEDGER_TARGET}
        EXPORT "${TARGETS_EXPORT_NAME}"
        LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
        INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)

include(InstallInclude)
# for doxygen
include(BuildDocs)

# for code coverage
if (COVERAGE)
    include(Coverage)
    config_coverage("'/usr*' '${CMAKE_SOURCE_DIR}/test/bcos-test*'")
endif ()
