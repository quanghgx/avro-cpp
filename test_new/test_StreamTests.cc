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
#include "boost/filesystem.hpp"
#include "Stream.hh"
#include "Exception.hh"

namespace avro {
  namespace stream {

    struct CheckEmpty1 {

      void operator()(InputStream& is) {
        const uint8_t* d;
        size_t n;
        REQUIRE(!is.next(&d, &n));
      }
    };

    struct CheckEmpty2 {

      void operator()(InputStream& is) {
        StreamReader r;
        r.reset(is);
        REQUIRE_THROWS_AS(r.read(), Exception);
      }
    };

    struct TestData {
      size_t chunkSize;
      size_t dataSize;
    };

    struct Fill1 {

      void operator()(OutputStream& os, size_t len) {
        StreamWriter w;
        w.reset(os);
        for (size_t i = 0; i < len; ++i) {
          w.write(i % 10 + '0');
        }
        w.flush();
      }
    };

    struct Fill2 {

      void operator()(OutputStream& os, size_t len) {
        for (size_t i = 0; i < len;) {
          uint8_t *b;
          size_t n;
          os.next(&b, &n);
          size_t j = 0;
          for (; i < len && j < n; ++j, ++i, ++b) {
            *b = i % 10 + '0';
          }
          if (i == len) {
            os.backup(n - j);
          }
        }
        os.flush();
      }
    };

    struct Verify1 {

      void operator()(InputStream& is, size_t dataSize) {
        StreamReader r;
        r.reset(is);
        for (size_t i = 0; i < dataSize; ++i) {
          REQUIRE((i % 10 + '0') == r.read());
        }
        REQUIRE_THROWS_AS(r.read(), Exception);
      }
    };

    struct Verify2 {

      void operator()(InputStream& is, size_t len) {
        const uint8_t *b;
        size_t n;

        for (size_t i = 0; i < len;) {
          REQUIRE(is.next(&b, &n));
          size_t j = 0;
          for (; i < len && j < n; ++j, ++i, ++b) {
            REQUIRE(*b == i % 10 + '0');
          }
          REQUIRE(j == n);
        }
        REQUIRE(!is.next(&b, &n));
      }
    };

    template <typename V>
    void testEmpty_memoryStream() {
      std::auto_ptr<OutputStream> os = memoryOutputStream();
      std::auto_ptr<InputStream> is = memoryInputStream(*os);
      V()(*is);
    }

    template <typename F, typename V>
    void testNonEmpty_memoryStream(const TestData& td) {
      std::auto_ptr<OutputStream> os = memoryOutputStream(td.chunkSize);
      F()(*os, td.dataSize);

      std::auto_ptr<InputStream> is = memoryInputStream(*os);
      V()(*is, td.dataSize);
    }

    void testNonEmpty2(const TestData& td) {
      std::vector<uint8_t> v;
      for (size_t i = 0; i < td.dataSize; ++i) {
        v.push_back(i % 10 + '0');
      }

      uint8_t v2 = 0;
      std::auto_ptr<InputStream> is = memoryInputStream(v.empty() ? &v2 : &v[0], v.size());
      Verify1()(*is, td.dataSize);
    }

    static const char filename[] = "test_str.bin";

    struct FileRemover {
      const boost::filesystem::path file;

      FileRemover(const char* filename) : file(filename) {
      }

      ~FileRemover() {
        boost::filesystem::remove(file);
      }
    };

    template <typename V>
    void testEmpty_fileStream() {
      FileRemover fr(filename);
      {
        std::auto_ptr<OutputStream> os = fileOutputStream(filename);
      }
      std::auto_ptr<InputStream> is = fileInputStream(filename);
      V()(*is);
    }

    template <typename F, typename V>
    void testNonEmpty_fileStream(const TestData& td) {
      FileRemover fr(filename);
      {
        std::auto_ptr<OutputStream> os = fileOutputStream(filename,
          td.chunkSize);
        F()(*os, td.dataSize);
      }

      std::auto_ptr<InputStream> is = fileInputStream(filename, td.chunkSize);
      V()(*is, td.dataSize);
    }

    TestData data[] = {
      { 100, 0},
      { 100, 1},
      { 100, 10},
      { 100, 100},
      { 100, 101},
      { 100, 1000},
      { 100, 1024}
    };

  } // namespace stream

} // namespace

TEST_CASE("Avro C++ unit test suite for streams", "[all]") {
  avro::stream::testEmpty_memoryStream<avro::stream::CheckEmpty1>();
  avro::stream::testEmpty_memoryStream<avro::stream::CheckEmpty2>();

  for (auto& item : avro::stream::data) avro::stream::testNonEmpty_memoryStream<avro::stream::Fill1, avro::stream::Verify1>(item);
  for (auto& item : avro::stream::data) avro::stream::testNonEmpty_memoryStream<avro::stream::Fill2, avro::stream::Verify1>(item);
  for (auto& item : avro::stream::data) avro::stream::testNonEmpty_memoryStream<avro::stream::Fill2, avro::stream::Verify2>(item);

  for (auto& item : avro::stream::data) avro::stream::testNonEmpty2(item);

  avro::stream::testEmpty_fileStream<avro::stream::CheckEmpty1>();
  avro::stream::testEmpty_fileStream<avro::stream::CheckEmpty2>();

  for (auto& item : avro::stream::data) avro::stream::testNonEmpty_fileStream<avro::stream::Fill1, avro::stream::Verify1>(item);
  for (auto& item : avro::stream::data) avro::stream::testNonEmpty_fileStream<avro::stream::Fill2, avro::stream::Verify1>(item);
  for (auto& item : avro::stream::data) avro::stream::testNonEmpty_fileStream<avro::stream::Fill2, avro::stream::Verify2>(item);
}
