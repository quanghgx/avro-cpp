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
#include <memory>
#include "Specific.hh"
#include "Stream.hh"

using std::string;
using std::vector;
using std::map;
using boost::array;

namespace avro {

  class C {
    int32_t i_;
    int64_t l_;
  public:

    C() : i_(0), l_(0) {
    }

    C(int32_t i, int64_t l) : i_(i), l_(l) {
    }

    int32_t i() const {
      return i_;
    }

    int64_t l() const {
      return l_;
    }

    void i(int32_t ii) {
      i_ = ii;
    }

    void l(int64_t ll) {
      l_ = ll;
    }

    bool operator==(const C& oth) const {
      return i_ == oth.i_ && l_ == oth.l_;
    }
  };

  template <> struct codec_traits<C> {

    static void encode(Encoder& e, const C& c) {
      e.encodeInt(c.i());
      e.encodeLong(c.l());
    }

    static void decode(Decoder& d, C& c) {
      c.i(d.decodeInt());
      c.l(d.decodeLong());
    }
  };

  namespace specific {

    class Test {
      std::shared_ptr<OutputStream> os;
      EncoderPtr e;
      DecoderPtr d;
    public:

      Test() : os(memoryOutputStream()), e(binaryEncoder()), d(binaryDecoder()) {
        e->init(*os);
      }

      template <typename T> void encode(const T& t) {
        avro::encode(*e, t);
        e->flush();
      }

      template <typename T> void decode(T& t) {
        std::shared_ptr<InputStream> is = memoryInputStream(*os);
        d->init(*is);
        avro::decode(*d, t);
      }
    };

    template <typename T> T encodeAndDecode(const T& t) {
      Test tst;

      tst.encode(t);

      T actual = T();

      tst.decode(actual);
      return actual;
    }

    TEST_CASE("Specific tests: testBool", "[testBool]") {
      bool b = encodeAndDecode(true);
      REQUIRE(b == true);
    }

    TEST_CASE("Specific tests: testInt", "[testInt]") {
      int32_t n = 10;
      int32_t b = encodeAndDecode(n);
      REQUIRE(b == n);
    }

    TEST_CASE("Specific tests: testLong", "[testLong]") {
      int64_t n = -109;
      int64_t b = encodeAndDecode(n);
      REQUIRE(b == n);
    }

    TEST_CASE("Specific tests: testFloat", "[testFloat]") {
      float n = 10.19f;
      float b = encodeAndDecode(n);
      REQUIRE(std::abs(b - n) < 0.00001f);
    }

    TEST_CASE("Specific tests: testDouble", "[testDouble]") {
      double n = 10.00001;
      double b = encodeAndDecode(n);
      REQUIRE(std::abs(b - n) < 0.00000001);
    }

    TEST_CASE("Specific tests: testString", "[testString]") {
      string n = "abc";
      string b = encodeAndDecode(n);
      REQUIRE(b == n);
    }

    TEST_CASE("Specific tests: testBytes", "[testBytes]") {
      uint8_t values[] = {1, 7, 23, 47, 83};
      vector<uint8_t> n(values, values + 5);
      vector<uint8_t> b = encodeAndDecode(n);
      REQUIRE(b == n);
    }

   
    TEST_CASE("Specific tests: testCustom", "[testCustom]") {
      C n(10, 1023);
      C b = encodeAndDecode(n);
      REQUIRE(b == n);
    }

  }
}
