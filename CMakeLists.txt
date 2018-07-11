#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
cmake_minimum_required(VERSION 3.8)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -Wall -Wextra")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set (AVRO_VERSION_MAJOR 1.8.2)
set (AVRO_VERSION_MINOR "0")

project (Avro-cpp)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR})

find_package (Boost 1.38 REQUIRED  COMPONENTS filesystem system program_options iostreams)
include_directories (3rd api ${CMAKE_CURRENT_BINARY_DIR} ${Boost_INCLUDE_DIRS})

file (GLOB_RECURSE AVRO_SOURCE_FILES "impl/*.cc")

add_library (avrocpp SHARED ${AVRO_SOURCE_FILES})
set_property (TARGET avrocpp APPEND PROPERTY COMPILE_DEFINITIONS AVRO_DYN_LINK)

add_library (avrocpp_s STATIC ${AVRO_SOURCE_FILES})

set_property (TARGET avrocpp avrocpp_s APPEND PROPERTY COMPILE_DEFINITIONS AVRO_SOURCE)
set_target_properties (avrocpp PROPERTIES  VERSION ${AVRO_VERSION_MAJOR}.${AVRO_VERSION_MINOR})
set_target_properties (avrocpp_s PROPERTIES  VERSION ${AVRO_VERSION_MAJOR}.${AVRO_VERSION_MINOR})

target_link_libraries (avrocpp ${Boost_LIBRARIES})

# -----------------------------------------------------------------------
# Tools
#
add_executable (precompile test/precompile.cc)
target_link_libraries (precompile avrocpp_s ${Boost_LIBRARIES})

add_executable (avrogencpp impl/avrogencpp.cc)
target_link_libraries (avrogencpp avrocpp_s ${Boost_LIBRARIES})

# -----------------------------------------------------------------------
# Examples
#
add_executable (example_custom examples/custom.cc)
target_link_libraries (example_custom avrocpp_s ${Boost_LIBRARIES})

add_executable (example_datafile examples/datafile.cc)
target_link_libraries (example_datafile avrocpp_s ${Boost_LIBRARIES})

add_executable (example_generated examples/generated.cc)
target_link_libraries (example_generated avrocpp_s ${Boost_LIBRARIES})

add_executable (example_generic examples/generic.cc)
target_link_libraries (example_generic avrocpp_s ${Boost_LIBRARIES})

add_executable (example_resolving examples/resolving.cc)
target_link_libraries (example_resolving avrocpp_s ${Boost_LIBRARIES})

add_executable (example_schemaload examples/schemaload.cc)
target_link_libraries (example_schemaload avrocpp_s ${Boost_LIBRARIES})

add_executable (example_validating examples/validating.cc)
target_link_libraries (example_validating avrocpp_s ${Boost_LIBRARIES})

# -----------------------------------------------------------------------
# Installation
#
set (CPACK_PACKAGE_FILE_NAME "avrocpp-${AVRO_VERSION_MAJOR}")

include (CPack)

install (TARGETS avrocpp avrocpp_s
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION lib)

install (TARGETS avrogencpp RUNTIME DESTINATION bin)

install (DIRECTORY api/ DESTINATION include/avro
    FILES_MATCHING PATTERN *.hh)

# -----------------------------------------------------------------------
# Unit tests (with catch2)
#
file (GLOB_RECURSE CATCH2_TESTS "test_new/*.cc")
add_executable (catch2_test ${CATCH2_TESTS})
target_link_libraries (catch2_test avrocpp_s ${Boost_LIBRARIES})