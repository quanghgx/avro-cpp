#include <catch.hpp>
#include <string>
#include <vector>
#include <iostream>

#include "../impl/json/JsonDom.hh"

using std::string;
using std::vector;
using namespace avro::json;

TEST_CASE("Avro C++ unit tests for json routines: testNull", "[json,null]") {
    Entity n = loadEntity("null");
    REQUIRE(n.type() == etNull);
}