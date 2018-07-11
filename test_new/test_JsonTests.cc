/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <catch.hpp>
#include <string>
#include <vector>
#include <cmath>
#include <iostream>
#include "../impl/json/JsonDom.hh"

using std::string;
using std::vector;
using namespace avro::json;

TEST_CASE("Avro C++ unit tests for json routines: testNull", "[testNull]") {
  Entity n = loadEntity("null");
  REQUIRE(n.type() == etNull);
}

TEST_CASE("Avro C++ unit tests for json routines: testArray0", "[testArray0]") {
  Entity n = loadEntity("[]");
  REQUIRE(n.type() == etArray);
  const Array& a = n.arrayValue();
  REQUIRE(a.size() == 0);
}

TEST_CASE("Avro C++ unit tests for json routines: testArray1", "[testArray1]") {
  Entity n = loadEntity("[200]");
  REQUIRE(n.type() == etArray);
  const Array& a = n.arrayValue();
  REQUIRE(a.size() == 1);
  REQUIRE(a[0].type() == etLong);
  REQUIRE(a[0].longValue() == 200ll);
}

TEST_CASE("Avro C++ unit tests for json routines: testArray2", "[testArray2]") {
  Entity n = loadEntity("[200, \"v100\"]");
  REQUIRE(n.type() == etArray);
  const Array& a = n.arrayValue();
  REQUIRE(a.size() == 2);
  REQUIRE(a[0].type() == etLong);
  REQUIRE(a[0].longValue() == 200ll);
  REQUIRE(a[1].type() == etString);
  REQUIRE(a[1].stringValue() == "v100");
}

TEST_CASE("Avro C++ unit tests for json routines: testObject0", "[testObject0]") {
  Entity n = loadEntity("{}");
  REQUIRE(n.type() == etObject);
  const Object& m = n.objectValue();
  REQUIRE(m.size() == 0);
}

TEST_CASE("Avro C++ unit tests for json routines: testObject1", "[testObject1]") {
  Entity n = loadEntity("{\"k1\": 100}");
  REQUIRE(n.type() == etObject);
  const Object& m = n.objectValue();
  REQUIRE(m.size() == 1);
  REQUIRE(m.begin()->first == "k1");
  REQUIRE(m.begin()->second.type() == etLong);
  REQUIRE(m.begin()->second.longValue() == 100ll);
}

TEST_CASE("Avro C++ unit tests for json routines: testObject2", "[testObject2]") {
  Entity n = loadEntity("{\"k1\": 100, \"k2\": [400, \"v0\"]}");
  REQUIRE(n.type() == etObject);
  const Object& m = n.objectValue();
  REQUIRE(m.size() == 2);

  Object::const_iterator it = m.find("k1");
  REQUIRE(it != m.end());
  REQUIRE(it->second.type() == etLong);
  REQUIRE(m.begin()->second.longValue() == 100ll);

  it = m.find("k2");
  REQUIRE(it != m.end());
  REQUIRE(it->second.type() == etArray);
  const Array& a = it->second.arrayValue();
  REQUIRE(a.size() == 2);
  REQUIRE(a[0].type() == etLong);
  REQUIRE(a[0].longValue() == 400ll);
  REQUIRE(a[1].type() == etString);
  REQUIRE(a[1].stringValue() == "v0");
}

/* Generic tests with list of inputs*/

template <typename T>
struct TestData {
  const char *input;
  EntityType type;
  T value;
};

TestData<bool> boolData[] = {
  { "true", etBool, true},
  { "false", etBool, false},
};

TestData<int64_t> longData[] = {
  { "0", etLong, 0},
  { "-1", etLong, -1},
  { "1", etLong, 1},
  { "9223372036854775807", etLong, 9223372036854775807LL},
  { "-9223372036854775807", etLong, -9223372036854775807LL},
};

TestData<double> doubleData[] = {
  { "0.0", etDouble, 0.0},
  { "-1.0", etDouble, -1.0},
  { "1.0", etDouble, 1.0},
  { "4.7e3", etDouble, 4700.0},
  { "-7.2e-4", etDouble, -0.00072},
  { "1e4", etDouble, 10000},
  { "-1e-4", etDouble, -0.0001},
  { "-0e0", etDouble, 0.0},
};

TestData<const char*> stringData[] = {
  { "\"\"", etString, ""},
  { "\"a\"", etString, "a"},
  { "\"\\U000a\"", etString, "\n"},
  { "\"\\u000a\"", etString, "\n"},
  { "\"\\\"\"", etString, "\""},
  { "\"\\/\"", etString, "/"},
};

void testBool(const TestData<bool>& d) {
  Entity n = loadEntity(d.input);
  REQUIRE(n.type() == d.type);
  REQUIRE(n.boolValue() == d.value);
}

void testLong(const TestData<int64_t>& d) {
  Entity n = loadEntity(d.input);
  REQUIRE(n.type() == d.type);
  REQUIRE(n.longValue() == d.value);
}

void testDouble(const TestData<double>& d) {
  Entity n = loadEntity(d.input);
  REQUIRE(n.type() == d.type);
  REQUIRE(std::abs(n.doubleValue() - d.value) < 1e-10);
}

void testString(const TestData<const char*>& d) {
  Entity n = loadEntity(d.input);
  REQUIRE(n.type() == d.type);
  REQUIRE(n.stringValue() == d.value);
}

TEST_CASE("Avro C++ unit tests for json routines: other", "[other]") {
  for (auto& item : boolData) testBool(item);
  for (auto& item : longData) testLong(item);
  for (auto& item : doubleData) testDouble(item);
  for (auto& item : stringData) testString(item);
}