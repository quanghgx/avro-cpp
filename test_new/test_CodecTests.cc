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
#include <iostream>
#include <memory>
#include <cstdint>
#include <vector>
#include <stack>
#include <string>
#include <functional>

#include "Encoder.hh"
#include "Decoder.hh"
#include "Compiler.hh"
#include "ValidSchema.hh"
#include "Generic.hh"
#include "Specific.hh"

#include <boost/bind.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/math/special_functions/fpclassify.hpp>

namespace avro {

  /*
  void dump(const OutputStream& os)
  {
      std::shared_ptr<InputStream> in = memoryInputStream(os);
      const char *b;
      size_t n;
      std::cout << os.byteCount() << std::endl;
      while (in->next(reinterpret_cast<const uint8_t**>(&b), &n)) {
          std::cout << std::string(b, n);
      }
      std::cout << std::endl;
  }
   */

  namespace parsing {

    static const unsigned int count = 10;

    /**
     * A bunch of tests that share quite a lot of infrastructure between them.
     * The basic idea is to generate avro data according to a schema and
     * then read back and compare the data with the original. But quite a few
     * variations are possible:
     * 1. While reading back, one can skip different data elements
     * 2. While reading resolve against a reader's schema. The resolver may
     * promote data type, convert from union to plain data type and vice versa,
     * insert or remove fields in records or reorder fields in a record.
     * 
     * To test Json encoder and decoder, we use the same technique with only
     * one difference - we use JsonEncoder and JsonDecoder.
     *
     * We also use the same infrastructure to test GenericReader and GenericWriter.
     * In this case, avro binary is generated in the standard way. It is read
     * into a GenericDatum, which in turn is written out. This newly serialized
     * data is decoded in the standard way to check that it is what is written. The
     * last step won't work if there is schema for reading is different from
     * that for writing. This is because any reordering of fields would have
     * got fixed by the GenericDatum's decoding and encoding step.
     *
     * For most tests, the data is generated at random.
     */

    using std::string;
    using std::vector;
    using std::stack;
    using std::pair;
    using std::istringstream;
    using std::ostringstream;
    using std::back_inserter;
    using std::copy;

    template <typename T>
    T from_string(const std::string& s) {
      istringstream iss(s);
      T result;
      iss >> result;
      return result;
    }

    template <>
    vector<uint8_t> from_string(const std::string& s) {
      vector<uint8_t> result;
      result.reserve(s.size());
      copy(s.begin(), s.end(), back_inserter(result));
      return result;
    }

    template <typename T>
    std::string to_string(const T& t) {
      ostringstream oss;
      oss << t;
      return oss.str();
    }

    template <>
    std::string to_string(const vector<uint8_t>& t) {
      string result;
      copy(t.begin(), t.end(), back_inserter(result));
      return result;
    }

    class Scanner {
      const char *p;
      const char * const end;
    public:

      Scanner(const char* calls) : p(calls), end(calls + strlen(calls)) {
      }

      Scanner(const char* calls, size_t len) : p(calls), end(calls + len) {
      }

      char advance() {
        return *p++;
      }

      int extractInt() {
        int result = 0;
        while (p < end) {
          if (isdigit(*p)) {
            result *= 10;
            result += *p++ -'0';
          } else {
            break;
          }
        }
        return result;
      }

      bool isDone() const {
        return p == end;
      }
    };

    boost::mt19937 rnd;

    static string randomString(size_t len) {
      std::string result;
      result.reserve(len + 1);
      for (size_t i = 0; i < len; ++i) {
        char c = static_cast<char> (rnd()) & 0x7f;
        if (c == '\0') {
          c = '\x7f';
        }
        result.push_back(c);
      }
      return result;
    }

    static vector<uint8_t> randomBytes(size_t len) {
      vector<uint8_t> result;
      result.reserve(len);
      for (size_t i = 0; i < len; ++i) {
        result.push_back(rnd());
      }
      return result;
    }

    static vector<string> randomValues(const char* calls) {
      Scanner sc(calls);
      vector<string> result;
      while (!sc.isDone()) {
        char c = sc.advance();
        switch (c) {
          case 'B':
            result.push_back(to_string(rnd() % 2 == 0));
            break;
          case 'I':
            result.push_back(to_string(static_cast<int32_t> (rnd())));
            break;
          case 'L':
            result.push_back(to_string(rnd() | static_cast<int64_t> (rnd()) << 32));
            break;
          case 'F':
            result.push_back(
              to_string(static_cast<float> (rnd()) / static_cast<float> (rnd())));
            break;
          case 'D':
            result.push_back(
              to_string(static_cast<double> (rnd()) / static_cast<double> (rnd())));
            break;
          case 'S':
          case 'K':
            result.push_back(to_string(randomString(sc.extractInt())));
            break;
          case 'b':
          case 'f':
            result.push_back(to_string(randomBytes(sc.extractInt())));
            break;
          case 'e':
          case 'c':
          case 'U':
            sc.extractInt();
            break;
          case 'N':
          case '[':
          case ']':
          case '{':
          case '}':
          case 's':
            break;
          default:
            throw std::runtime_error("Unknown mnemonic");
        }
      }
      return result;
    }

    static std::shared_ptr<OutputStream> generate(Encoder& e, const char* calls,
      const vector<string>& values) {
      Scanner sc(calls);
      vector<string>::const_iterator it = values.begin();
      std::shared_ptr<OutputStream> ob = memoryOutputStream();
      e.init(*ob);

      while (!sc.isDone()) {
        char c = sc.advance();

        switch (c) {
          case 'N':
            e.encodeNull();
            break;
          case 'B':
            e.encodeBool(from_string<bool>(*it++));
            break;
          case 'I':
            e.encodeInt(from_string<int32_t>(*it++));
            break;
          case 'L':
            e.encodeLong(from_string<int64_t>(*it++));
            break;
          case 'F':
            e.encodeFloat(from_string<float>(*it++));
            break;
          case 'D':
            e.encodeDouble(from_string<double>(*it++));
            break;
          case 'S':
          case 'K':
            sc.extractInt();
            e.encodeString(from_string<string>(*it++));
            break;
          case 'b':
            sc.extractInt();
            e.encodeBytes(from_string<vector<uint8_t> >(*it++));
            break;          
          case 'c':
            e.setItemCount(sc.extractInt());
            break;
          case 's':
            e.startItem();
            break;          
          default:
            throw std::runtime_error("Unknown mnemonic");
        }
      }
      e.flush();
      return ob;
    }

    namespace {

      struct StackElement {
        size_t size;
        size_t count;
        bool isArray;

        StackElement(size_t s, bool a) : size(s), count(0), isArray(a) {
        }
      };
    }

    ValidSchema makeValidSchema(const char* schema) {
      istringstream iss(schema);
      ValidSchema vs;
      compileJsonSchema(iss, vs);
      return ValidSchema(vs);
    }

    void testEncoder(const EncoderPtr& e, const char* writerCalls,
      vector<string>& v, std::shared_ptr<OutputStream>& p) {
      v = randomValues(writerCalls);
      p = generate(*e, writerCalls, v);
    }

    /**
     * The first member is a schema.
     * The second one is a sequence of (single character) mnemonics:
     * N  null
     * B  boolean
     * I  int
     * L  long
     * F  float
     * D  double
     * K followed by integer - key-name (and its length) in a map
     * S followed by integer - string and its length
     * b followed by integer - bytes and length
     * f followed by integer - fixed and length
     * c  Number of items to follow in an array/map.
     * U followed by integer - Union and its branch
     * e followed by integer - Enum and its value
     * [  Start array
     * ]  End array
     * {  Start map
     * }  End map
     * s  start item
     * R  Start of record in resolving situations. Client may call fieldOrder()
     */

    struct TestData {
      const char* schema;
      const char* calls;
      unsigned int depth;
    };

    struct TestData2 {
      const char* schema;
      const char* correctCalls;
      const char* incorrectCalls;
      unsigned int depth;
    };

    struct TestData3 {
      const char* writerSchema;
      const char* writerCalls;
      const char* readerSchema;
      const char* readerCalls;
      unsigned int depth;
    };

    struct TestData4 {
      const char* writerSchema;
      const char* writerCalls;
      const char* writerValues[100];
      const char* readerSchema;
      const char* readerCalls;
      const char* readerValues[100];
      unsigned int depth;
    };

    template<typename CodecFactory>
    void testCodec(const TestData& td) {
      static int testNo = 0;
      testNo++;

      ValidSchema vs = makeValidSchema(td.schema);

      for (unsigned int i = 0; i < count; ++i) {
        vector<string> v;
        std::shared_ptr<OutputStream> p;
        testEncoder(CodecFactory::newEncoder(vs), td.calls, v, p);
        // dump(*p);

        for (unsigned int i = 0; i <= td.depth; ++i) {
          unsigned int skipLevel = td.depth - i;
          std::cout << "Test: " << testNo << ' '
            << " schema: " << td.schema
            << " calls: " << td.calls
            << " skip-level: " << skipLevel << "\n";
          std::shared_ptr<InputStream> in = memoryInputStream(*p);
          testDecoder(CodecFactory::newDecoder(vs), v, *in,
            td.calls, skipLevel);
        }
      }
    }

    static const TestData data[] = {
      { "\"null\"", "N", 1},
      { "\"boolean\"", "B", 1},
      { "\"int\"", "I", 1},
      { "\"long\"", "L", 1},
      { "\"float\"", "F", 1},
      { "\"double\"", "D", 1},
      { "\"string\"", "S0", 1},
      { "\"string\"", "S10", 1},
      { "\"bytes\"", "b0", 1},
      { "\"bytes\"", "b10", 1},

      { "{\"type\":\"fixed\", \"name\":\"fi\", \"size\": 1}", "f1", 1},
      { "{\"type\":\"fixed\", \"name\":\"fi\", \"size\": 10}", "f10", 1},
      { "{\"type\":\"enum\", \"name\":\"en\", \"symbols\":[\"v1\", \"v2\"]}",
        "e1", 1},

      { "{\"type\":\"array\", \"items\": \"boolean\"}", "[]", 2},
      { "{\"type\":\"array\", \"items\": \"int\"}", "[]", 2},
      { "{\"type\":\"array\", \"items\": \"long\"}", "[]", 2},
      { "{\"type\":\"array\", \"items\": \"float\"}", "[]", 2},
      { "{\"type\":\"array\", \"items\": \"double\"}", "[]", 2},
      { "{\"type\":\"array\", \"items\": \"string\"}", "[]", 2},
      { "{\"type\":\"array\", \"items\": \"bytes\"}", "[]", 2},
      { "{\"type\":\"array\", \"items\":{\"type\":\"fixed\", "
        "\"name\":\"fi\", \"size\": 10}}", "[]", 2},

      { "{\"type\":\"array\", \"items\": \"boolean\"}", "[c1sB]", 2},
      { "{\"type\":\"array\", \"items\": \"int\"}", "[c1sI]", 2},
      { "{\"type\":\"array\", \"items\": \"long\"}", "[c1sL]", 2},
      { "{\"type\":\"array\", \"items\": \"float\"}", "[c1sF]", 2},
      { "{\"type\":\"array\", \"items\": \"double\"}", "[c1sD]", 2},
      { "{\"type\":\"array\", \"items\": \"string\"}", "[c1sS10]", 2},
      { "{\"type\":\"array\", \"items\": \"bytes\"}", "[c1sb10]", 2},
      { "{\"type\":\"array\", \"items\": \"int\"}", "[c1sIc1sI]", 2},
      { "{\"type\":\"array\", \"items\": \"int\"}", "[c2sIsI]", 2},
      { "{\"type\":\"array\", \"items\":{\"type\":\"fixed\", "
        "\"name\":\"fi\", \"size\": 10}}", "[c2sf10sf10]", 2},

      { "{\"type\":\"map\", \"values\": \"boolean\"}", "{}", 2},
      { "{\"type\":\"map\", \"values\": \"int\"}", "{}", 2},
      { "{\"type\":\"map\", \"values\": \"long\"}", "{}", 2},
      { "{\"type\":\"map\", \"values\": \"float\"}", "{}", 2},
      { "{\"type\":\"map\", \"values\": \"double\"}", "{}", 2},
      { "{\"type\":\"map\", \"values\": \"string\"}", "{}", 2},
      { "{\"type\":\"map\", \"values\": \"bytes\"}", "{}", 2},
      { "{\"type\":\"map\", \"values\": "
        "{\"type\":\"array\", \"items\":\"int\"}}", "{}", 2},

      { "{\"type\":\"map\", \"values\": \"boolean\"}", "{c1sK5B}", 2},
      { "{\"type\":\"map\", \"values\": \"int\"}", "{c1sK5I}", 2},
      { "{\"type\":\"map\", \"values\": \"long\"}", "{c1sK5L}", 2},
      { "{\"type\":\"map\", \"values\": \"float\"}", "{c1sK5F}", 2},
      { "{\"type\":\"map\", \"values\": \"double\"}", "{c1sK5D}", 2},
      { "{\"type\":\"map\", \"values\": \"string\"}", "{c1sK5S10}", 2},
      { "{\"type\":\"map\", \"values\": \"bytes\"}", "{c1sK5b10}", 2},
      { "{\"type\":\"map\", \"values\": "
        "{\"type\":\"array\", \"items\":\"int\"}}", "{c1sK5[c3sIsIsI]}", 2},

      { "{\"type\":\"map\", \"values\": \"boolean\"}",
        "{c1sK5Bc2sK5BsK5B}", 2},

      { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f\", \"type\":\"boolean\"}]}", "B", 1},
      { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f\", \"type\":\"int\"}]}", "I", 1},
      { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f\", \"type\":\"long\"}]}", "L", 1},
      { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f\", \"type\":\"float\"}]}", "F", 1},
      { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f\", \"type\":\"double\"}]}", "D", 1},
      { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f\", \"type\":\"string\"}]}", "S10", 1},
      { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f\", \"type\":\"bytes\"}]}", "b10", 1},

      // multi-field records
      { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f1\", \"type\":\"int\"},"
        "{\"name\":\"f2\", \"type\":\"double\"},"
        "{\"name\":\"f3\", \"type\":\"string\"}]}", "IDS10", 1},
      { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f0\", \"type\":\"null\"},"
        "{\"name\":\"f1\", \"type\":\"boolean\"},"
        "{\"name\":\"f2\", \"type\":\"int\"},"
        "{\"name\":\"f3\", \"type\":\"long\"},"
        "{\"name\":\"f4\", \"type\":\"float\"},"
        "{\"name\":\"f5\", \"type\":\"double\"},"
        "{\"name\":\"f6\", \"type\":\"string\"},"
        "{\"name\":\"f7\", \"type\":\"bytes\"}]}",
        "NBILFDS10b25", 1},

      // record of records
      { "{\"type\":\"record\",\"name\":\"outer\",\"fields\":["
        "{\"name\":\"f1\", \"type\":{\"type\":\"record\", "
        "\"name\":\"inner\", \"fields\":["
        "{\"name\":\"g1\", \"type\":\"int\"}, {\"name\":\"g2\", "
        "\"type\":\"double\"}]}},"
        "{\"name\":\"f2\", \"type\":\"string\"},"
        "{\"name\":\"f3\", \"type\":\"inner\"}]}",
        "IDS10ID", 1},

      // record with name references
      { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f1\", \"type\":{\"type\":\"fixed\", "
        "\"name\":\"f\", \"size\":10 }},"
        "{\"name\":\"f2\", \"type\":\"f\"},"
        "{\"name\":\"f3\", \"type\":\"f\"}]}",
        "f10f10f10", 1},
      { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f1\", \"type\":{\"type\":\"enum\", "
        "\"name\": \"e\", \"symbols\":[\"s1\", \"s2\"] }},"
        "{\"name\":\"f2\", \"type\":\"e\"},"
        "{\"name\":\"f3\", \"type\":\"e\"}]}",
        "e1e0e1", 1},

      // record with array
      { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f1\", \"type\":\"long\"},"
        "{\"name\":\"f2\", "
        "\"type\":{\"type\":\"array\", \"items\":\"int\"}}]}",
        "L[c1sI]", 2},

      // record with map
      { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f1\", \"type\":\"long\"},"
        "{\"name\":\"f2\", "
        "\"type\":{\"type\":\"map\", \"values\":\"int\"}}]}",
        "L{c1sK5I}", 2},

      // array of records
      { "{\"type\":\"array\", \"items\":"
        "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f1\", \"type\":\"long\"},"
        "{\"name\":\"f2\", \"type\":\"null\"}]}}",
        "[c2sLNsLN]", 2},


      { "{\"type\":\"array\", \"items\":"
        "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f1\", \"type\":\"long\"},"
        "{\"name\":\"f2\", "
        "\"type\":{\"type\":\"array\", \"items\":\"int\"}}]}}",
        "[c2sL[c1sI]sL[c2sIsI]]", 3},
      { "{\"type\":\"array\", \"items\":"
        "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f1\", \"type\":\"long\"},"
        "{\"name\":\"f2\", "
        "\"type\":{\"type\":\"map\", \"values\":\"int\"}}]}}",
        "[c2sL{c1sK5I}sL{c2sK5IsK5I}]", 3},
      { "{\"type\":\"array\", \"items\":"
        "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f1\", \"type\":\"long\"},"
        "{\"name\":\"f2\", "
        "\"type\":[\"null\", \"int\"]}]}}",
        "[c2sLU0NsLU1I]", 2},

      { "[\"boolean\", \"null\" ]", "U0B", 1},
      { "[\"int\", \"null\" ]", "U0I", 1},
      { "[\"long\", \"null\" ]", "U0L", 1},
      { "[\"float\", \"null\" ]", "U0F", 1},
      { "[\"double\", \"null\" ]", "U0D", 1},
      { "[\"string\", \"null\" ]", "U0S10", 1},
      { "[\"bytes\", \"null\" ]", "U0b10", 1},

      { "[\"null\", \"int\"]", "U0N", 1},
      { "[\"boolean\", \"int\"]", "U0B", 1},
      { "[\"boolean\", \"int\"]", "U1I", 1},
      { "[\"boolean\", {\"type\":\"array\", \"items\":\"int\"} ]",
        "U0B", 1},

      { "[\"boolean\", {\"type\":\"array\", \"items\":\"int\"} ]",
        "U1[c1sI]", 2},

      // Recursion
      { "{\"type\": \"record\", \"name\": \"Node\", \"fields\": ["
        "{\"name\":\"label\", \"type\":\"string\"},"
        "{\"name\":\"children\", \"type\":"
        "{\"type\": \"array\", \"items\": \"Node\" }}]}",
        "S10[c1sS10[]]", 3},

      { "{\"type\": \"record\", \"name\": \"Lisp\", \"fields\": ["
        "{\"name\":\"value\", \"type\":[\"null\", \"string\","
        "{\"type\": \"record\", \"name\": \"Cons\", \"fields\": ["
        "{\"name\":\"car\", \"type\":\"Lisp\"},"
        "{\"name\":\"cdr\", \"type\":\"Lisp\"}]}]}]}",
        "U0N", 1},
      { "{\"type\": \"record\", \"name\": \"Lisp\", \"fields\": ["
        "{\"name\":\"value\", \"type\":[\"null\", \"string\","
        "{\"type\": \"record\", \"name\": \"Cons\", \"fields\": ["
        "{\"name\":\"car\", \"type\":\"Lisp\"},"
        "{\"name\":\"cdr\", \"type\":\"Lisp\"}]}]}]}",
        "U1S10", 1},
      { "{\"type\": \"record\", \"name\": \"Lisp\", \"fields\": ["
        "{\"name\":\"value\", \"type\":[\"null\", \"string\","
        "{\"type\": \"record\", \"name\": \"Cons\", \"fields\": ["
        "{\"name\":\"car\", \"type\":\"Lisp\"},"
        "{\"name\":\"cdr\", \"type\":\"Lisp\"}]}]}]}",
        "U2U1S10U0N", 1},
    };

    static const TestData2 data2[] = {
      { "\"int\"", "I", "B", 1},
      { "\"boolean\"", "B", "I", 1},
      { "\"boolean\"", "B", "L", 1},
      { "\"boolean\"", "B", "F", 1},
      { "\"boolean\"", "B", "D", 1},
      { "\"boolean\"", "B", "S10", 1},
      { "\"boolean\"", "B", "b10", 1},
      { "\"boolean\"", "B", "[]", 1},
      { "\"boolean\"", "B", "{}", 1},
      { "\"boolean\"", "B", "U0", 1},
      { "{\"type\":\"fixed\", \"name\":\"fi\", \"size\": 1}", "f1", "f2", 1},
    };

    static const TestData3 data3[] = {
      { "\"int\"", "I", "\"float\"", "F", 1},
      { "\"int\"", "I", "\"double\"", "D", 1},
      { "\"int\"", "I", "\"long\"", "L", 1},
      { "\"long\"", "L", "\"float\"", "F", 1},
      { "\"long\"", "L", "\"double\"", "D", 1},
      { "\"float\"", "F", "\"double\"", "D", 1},

      { "{\"type\":\"array\", \"items\": \"int\"}", "[]",
        "{\"type\":\"array\", \"items\": \"long\"}", "[]", 2},
      { "{\"type\":\"array\", \"items\": \"int\"}", "[]",
        "{\"type\":\"array\", \"items\": \"double\"}", "[]", 2},
      { "{\"type\":\"array\", \"items\": \"long\"}", "[]",
        "{\"type\":\"array\", \"items\": \"double\"}", "[]", 2},
      { "{\"type\":\"array\", \"items\": \"float\"}", "[]",
        "{\"type\":\"array\", \"items\": \"double\"}", "[]", 2},

      { "{\"type\":\"array\", \"items\": \"int\"}", "[c1sI]",
        "{\"type\":\"array\", \"items\": \"long\"}", "[c1sL]", 2},
      { "{\"type\":\"array\", \"items\": \"int\"}", "[c1sI]",
        "{\"type\":\"array\", \"items\": \"double\"}", "[c1sD]", 2},
      { "{\"type\":\"array\", \"items\": \"long\"}", "[c1sL]",
        "{\"type\":\"array\", \"items\": \"double\"}", "[c1sD]", 2},
      { "{\"type\":\"array\", \"items\": \"float\"}", "[c1sF]",
        "{\"type\":\"array\", \"items\": \"double\"}", "[c1sD]", 2},

      { "{\"type\":\"map\", \"values\": \"int\"}", "{}",
        "{\"type\":\"map\", \"values\": \"long\"}", "{}", 2},
      { "{\"type\":\"map\", \"values\": \"int\"}", "{}",
        "{\"type\":\"map\", \"values\": \"double\"}", "{}", 2},
      { "{\"type\":\"map\", \"values\": \"long\"}", "{}",
        "{\"type\":\"map\", \"values\": \"double\"}", "{}", 2},
      { "{\"type\":\"map\", \"values\": \"float\"}", "{}",
        "{\"type\":\"map\", \"values\": \"double\"}", "{}", 2},

      { "{\"type\":\"map\", \"values\": \"int\"}", "{c1sK5I}",
        "{\"type\":\"map\", \"values\": \"long\"}", "{c1sK5L}", 2},
      { "{\"type\":\"map\", \"values\": \"int\"}", "{c1sK5I}",
        "{\"type\":\"map\", \"values\": \"double\"}", "{c1sK5D}", 2},
      { "{\"type\":\"map\", \"values\": \"long\"}", "{c1sK5L}",
        "{\"type\":\"map\", \"values\": \"double\"}", "{c1sK5D}", 2},
      { "{\"type\":\"map\", \"values\": \"float\"}", "{c1sK5F}",
        "{\"type\":\"map\", \"values\": \"double\"}", "{c1sK5D}", 2},

      { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f\", \"type\":\"int\"}]}", "I",
        "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f\", \"type\":\"long\"}]}", "L", 1},
      { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f\", \"type\":\"int\"}]}", "I",
        "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f\", \"type\":\"double\"}]}", "D", 1},

      // multi-field record with promotions
      { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f0\", \"type\":\"boolean\"},"
        "{\"name\":\"f1\", \"type\":\"int\"},"
        "{\"name\":\"f2\", \"type\":\"float\"},"
        "{\"name\":\"f3\", \"type\":\"string\"}]}", "BIFS",
        "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f0\", \"type\":\"boolean\"},"
        "{\"name\":\"f1\", \"type\":\"long\"},"
        "{\"name\":\"f2\", \"type\":\"double\"},"
        "{\"name\":\"f3\", \"type\":\"string\"}]}", "BLDS", 1},

      { "[\"int\", \"long\"]", "U0I", "[\"long\", \"string\"]", "U0L", 1},
      { "[\"int\", \"long\"]", "U0I", "[\"double\", \"string\"]", "U0D", 1},
      { "[\"long\", \"double\"]", "U0L", "[\"double\", \"string\"]", "U0D", 1},
      { "[\"float\", \"double\"]", "U0F", "[\"double\", \"string\"]", "U0D", 1},

      { "\"int\"", "I", "[\"int\", \"string\"]", "U0I", 1},

      { "[\"int\", \"double\"]", "U0I", "\"int\"", "I", 1},
      { "[\"int\", \"double\"]", "U0I", "\"long\"", "L", 1},

      { "[\"boolean\", \"int\"]", "U1I", "[\"boolean\", \"long\"]", "U1L", 1},
      { "[\"boolean\", \"int\"]", "U1I", "[\"long\", \"boolean\"]", "U0L", 1},
    };

    static const TestData4 data4[] = {
      // Projection
      { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f1\", \"type\":\"string\"},"
        "{\"name\":\"f2\", \"type\":\"string\"},"
        "{\"name\":\"f3\", \"type\":\"int\"}]}", "S10S10IS10S10I",
        { "s1", "s2", "100", "t1", "t2", "200", NULL},
        "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f1\", \"type\":\"string\" },"
        "{\"name\":\"f2\", \"type\":\"string\"}]}", "RS10S10RS10S10",
        { "s1", "s2", "t1", "t2", NULL}, 1},

      // Reordered fields
      { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f1\", \"type\":\"int\"},"
        "{\"name\":\"f2\", \"type\":\"string\"}]}", "IS10",
        { "10", "hello", NULL},
        "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f2\", \"type\":\"string\" },"
        "{\"name\":\"f1\", \"type\":\"long\"}]}", "RLS10",
        { "10", "hello", NULL}, 1},

      // Default values
      { "{\"type\":\"record\",\"name\":\"r\",\"fields\":[]}", "",
        { NULL},
        "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f\", \"type\":\"int\", \"default\": 100}]}", "RI",
        { "100", NULL}, 1},

      { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f2\", \"type\":\"int\"}]}", "I",
        { "10", NULL},
        "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f1\", \"type\":\"int\", \"default\": 101},"
        "{\"name\":\"f2\", \"type\":\"int\"}]}", "RII",
        { "10", "101", NULL}, 1},

      { "{\"type\":\"record\",\"name\":\"outer\",\"fields\":["
        "{\"name\": \"g1\", "
        "\"type\":{\"type\":\"record\",\"name\":\"inner\",\"fields\":["
        "{\"name\":\"f2\", \"type\":\"int\"}]}}, "
        "{\"name\": \"g2\", \"type\": \"long\"}]}", "IL",
        { "10", "11", NULL},
        "{\"type\":\"record\",\"name\":\"outer\",\"fields\":["
        "{\"name\": \"g1\", "
        "\"type\":{\"type\":\"record\",\"name\":\"inner\",\"fields\":["
        "{\"name\":\"f1\", \"type\":\"int\", \"default\": 101},"
        "{\"name\":\"f2\", \"type\":\"int\"}]}}, "
        "{\"name\": \"g2\", \"type\": \"long\"}]}}", "RRIIL",
        { "10", "101", "11", NULL}, 1},

      // Default value for a record.
      { "{\"type\":\"record\",\"name\":\"outer\",\"fields\":["
        "{\"name\": \"g1\", "
        "\"type\":{\"type\":\"record\",\"name\":\"inner1\",\"fields\":["
        "{\"name\":\"f1\", \"type\":\"long\" },"
        "{\"name\":\"f2\", \"type\":\"int\"}] } }, "
        "{\"name\": \"g2\", \"type\": \"long\"}]}", "LIL",
        { "10", "12", "13", NULL},
        "{\"type\":\"record\",\"name\":\"outer\",\"fields\":["
        "{\"name\": \"g1\", "
        "\"type\":{\"type\":\"record\",\"name\":\"inner1\",\"fields\":["
        "{\"name\":\"f1\", \"type\":\"long\" },"
        "{\"name\":\"f2\", \"type\":\"int\"}] } }, "
        "{\"name\": \"g2\", \"type\": \"long\"},"
        "{\"name\": \"g3\", "
        "\"type\":{\"type\":\"record\",\"name\":\"inner2\",\"fields\":["
        "{\"name\":\"f1\", \"type\":\"long\" },"
        "{\"name\":\"f2\", \"type\":\"int\"}] }, "
        "\"default\": { \"f1\": 15, \"f2\": 101 } }] } ",
        "RRLILRLI",
        { "10", "12", "13", "15", "101", NULL}, 1},

      { "{\"type\":\"record\",\"name\":\"outer\",\"fields\":["
        "{\"name\": \"g1\", "
        "\"type\":{\"type\":\"record\",\"name\":\"inner1\",\"fields\":["
        "{\"name\":\"f1\", \"type\":\"long\" },"
        "{\"name\":\"f2\", \"type\":\"int\"}] } }, "
        "{\"name\": \"g2\", \"type\": \"long\"}]}", "LIL",
        { "10", "12", "13", NULL},
        "{\"type\":\"record\",\"name\":\"outer\",\"fields\":["
        "{\"name\": \"g1\", "
        "\"type\":{\"type\":\"record\",\"name\":\"inner1\",\"fields\":["
        "{\"name\":\"f1\", \"type\":\"long\" },"
        "{\"name\":\"f2\", \"type\":\"int\"}] } }, "
        "{\"name\": \"g2\", \"type\": \"long\"},"
        "{\"name\": \"g3\", "
        "\"type\":\"inner1\", "
        "\"default\": { \"f1\": 15, \"f2\": 101 } }] } ",
        "RRLILRLI",
        { "10", "12", "13", "15", "101", NULL}, 1},

      { "{\"type\":\"record\",\"name\":\"r\",\"fields\":[]}", "",
        { NULL},
        "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f\", \"type\":{ \"type\": \"array\", \"items\": \"int\" },"
        "\"default\": [100]}]}", "[c1sI]",
        { "100", NULL}, 1},

      { "{ \"type\": \"array\", \"items\": {\"type\":\"record\","
        "\"name\":\"r\",\"fields\":["
        "{\"name\":\"f0\", \"type\": \"int\"}]} }", "[c1sI]",
        { "99", NULL},
        "{ \"type\": \"array\", \"items\": {\"type\":\"record\","
        "\"name\":\"r\",\"fields\":["
        "{\"name\":\"f\", \"type\":\"int\", \"default\": 100}]} }",
        "[Rc1sI]",
        { "100", NULL}, 1},

      // Record of array of record with deleted field as last field
      { "{\"type\":\"record\",\"name\":\"outer\",\"fields\":["
        "{\"name\": \"g1\","
        "\"type\":{\"type\":\"array\",\"items\":{"
        "\"name\":\"item\",\"type\":\"record\",\"fields\":["
        "{\"name\":\"f1\", \"type\":\"int\"},"
        "{\"name\":\"f2\", \"type\": \"long\", \"default\": 0}]}}}]}", "[c1sIL]",
        { "10", "11", NULL},
        "{\"type\":\"record\",\"name\":\"outer\",\"fields\":["
        "{\"name\": \"g1\","
        "\"type\":{\"type\":\"array\",\"items\":{"
        "\"name\":\"item\",\"type\":\"record\",\"fields\":["
        "{\"name\":\"f1\", \"type\":\"int\"}]}}}]}", "R[c1sI]",
        { "10", NULL}, 2},

      // Enum resolution
      { "{\"type\":\"enum\",\"name\":\"e\",\"symbols\":[\"x\",\"y\",\"z\"]}",
        "e2",
        { NULL},
        "{\"type\":\"enum\",\"name\":\"e\",\"symbols\":[ \"y\", \"z\" ]}",
        "e1",
        { NULL}, 1},

      { "{\"type\":\"enum\",\"name\":\"e\",\"symbols\":[ \"x\", \"y\" ]}",
        "e1",
        { NULL},
        "{\"type\":\"enum\",\"name\":\"e\",\"symbols\":[ \"y\", \"z\" ]}",
        "e0",
        { NULL}, 1},


      // Union
      { "\"int\"", "I",
        { "100", NULL},
        "[ \"long\", \"int\"]", "U1I",
        { "100", NULL}, 1},

      { "[ \"long\", \"int\"]", "U1I",
        { "100", NULL},
        "\"int\"", "I",
        { "100", NULL}, 1},

      // Arrray of unions
      { "{\"type\":\"array\", \"items\":[ \"long\", \"int\"]}",
        "[c2sU1IsU1I]",
        { "100", "100", NULL},
        "{\"type\":\"array\", \"items\": \"int\"}",
        "[c2sIsI]",
        { "100", "100", NULL}, 2},

      // Map of unions
      { "{\"type\":\"map\", \"values\":[ \"long\", \"int\"]}",
        "{c2sS10U1IsS10U1I}",
        { "k1", "100", "k2", "100", NULL},
        "{\"type\":\"map\", \"values\": \"int\"}",
        "{c2sS10IsS10I}",
        { "k1", "100", "k2", "100", NULL}, 2},

      // Union + promotion
      { "\"int\"", "I",
        { "100", NULL},
        "[ \"long\", \"string\"]", "U0L",
        { "100", NULL}, 1},

      { "[ \"int\", \"string\"]", "U0I",
        { "100", NULL},
        "\"long\"", "L",
        { "100", NULL}, 1},

      // Record where union field is skipped.
      { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f0\", \"type\":\"boolean\"},"
        "{\"name\":\"f1\", \"type\":\"int\"},"
        "{\"name\":\"f2\", \"type\":[\"int\", \"long\"]},"
        "{\"name\":\"f3\", \"type\":\"float\"}"
        "]}", "BIU0IF",
        { "1", "100", "121", "10.75", NULL},
        "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        "{\"name\":\"f0\", \"type\":\"boolean\"},"
        "{\"name\":\"f1\", \"type\":\"long\"},"
        "{\"name\":\"f3\", \"type\":\"double\"}]}", "BLD",
        { "1", "100", "10.75", NULL}, 1},
    };

    static const TestData4 data4BinaryOnly[] = {
      // Arrray of unions
      { "{\"type\":\"array\", \"items\":[ \"long\", \"int\"]}",
        "[c1sU1Ic1sU1I]",
        { "100", "100", NULL},
        "{\"type\":\"array\", \"items\": \"int\"}",
        "[c1sIc1sI]",
        { "100", "100", NULL}, 2},

      // Map of unions
      { "{\"type\":\"map\", \"values\":[ \"long\", \"int\"]}",
        "{c1sS10U1Ic1sS10U1I}",
        { "k1", "100", "k2", "100", NULL},
        "{\"type\":\"map\", \"values\": \"int\"}",
        "{c1sS10Ic1sS10I}",
        { "k1", "100", "k2", "100", NULL}, 2},
    };

    struct BinaryEncoderFactory {

      static EncoderPtr newEncoder(const ValidSchema& schema) {
        return binaryEncoder();
      }
    };

    struct BinaryDecoderFactory {

      static DecoderPtr newDecoder(const ValidSchema& schema) {
        return binaryDecoder();
      }
    };

    struct BinaryCodecFactory : public BinaryEncoderFactory,
    public BinaryDecoderFactory {
    };

    struct ValidatingEncoderFactory {

      static EncoderPtr newEncoder(const ValidSchema& schema) {
        return validatingEncoder(schema, binaryEncoder());
      }
    };

    struct ValidatingDecoderFactory {

      static DecoderPtr newDecoder(const ValidSchema& schema) {
        return validatingDecoder(schema, binaryDecoder());
      }
    };

    struct ValidatingCodecFactory : public ValidatingEncoderFactory,
    public ValidatingDecoderFactory {
    };

    struct JsonCodec {

      static EncoderPtr newEncoder(const ValidSchema& schema) {
        return jsonEncoder(schema);
      }

      static DecoderPtr newDecoder(const ValidSchema& schema) {
        return jsonDecoder(schema);
      }
    };

    struct JsonPrettyCodec {

      static EncoderPtr newEncoder(const ValidSchema& schema) {
        return jsonPrettyEncoder(schema);
      }

      static DecoderPtr newDecoder(const ValidSchema& schema) {
        return jsonDecoder(schema);
      }
    };

    struct BinaryEncoderResolvingDecoderFactory : public BinaryEncoderFactory {

      static DecoderPtr newDecoder(const ValidSchema& schema) {
        return resolvingDecoder(schema, schema, binaryDecoder());
      }

      static DecoderPtr newDecoder(const ValidSchema& writer,
        const ValidSchema& reader) {
        return resolvingDecoder(writer, reader, binaryDecoder());
      }
    };

    struct JsonEncoderResolvingDecoderFactory {

      static EncoderPtr newEncoder(const ValidSchema& schema) {
        return jsonEncoder(schema);
      }

      static DecoderPtr newDecoder(const ValidSchema& schema) {
        return resolvingDecoder(schema, schema, jsonDecoder(schema));
      }

      static DecoderPtr newDecoder(const ValidSchema& writer,
        const ValidSchema& reader) {
        return resolvingDecoder(writer, reader, jsonDecoder(writer));
      }
    };

    struct ValidatingEncoderResolvingDecoderFactory :
    public ValidatingEncoderFactory {

      static DecoderPtr newDecoder(const ValidSchema& schema) {
        return resolvingDecoder(schema, schema,
          validatingDecoder(schema, binaryDecoder()));
      }

      static DecoderPtr newDecoder(const ValidSchema& writer,
        const ValidSchema& reader) {
        return resolvingDecoder(writer, reader,
          validatingDecoder(writer, binaryDecoder()));
      }
    };

  } // namespace parsing

  TEST_CASE("Avro C++ unit tests for codecs: testStreamLifetimes", "[testStreamLifetimes]") {

    EncoderPtr e = binaryEncoder();
    {
      std::shared_ptr<OutputStream> s1 = memoryOutputStream();
      e->init(*s1);
      e->encodeInt(100);
      e->encodeDouble(4.73);
      e->flush();
    }

    {
      std::shared_ptr<OutputStream> s2 = memoryOutputStream();
      e->init(*s2);
      e->encodeDouble(3.14);
      e->flush();
    }

  }

  static void testLimits(const EncoderPtr& e, const DecoderPtr& d) {
    std::shared_ptr<OutputStream> s1 = memoryOutputStream();
    {
      e->init(*s1);
      e->encodeDouble(std::numeric_limits<double>::infinity());
      e->encodeDouble(-std::numeric_limits<double>::infinity());
      e->encodeDouble(std::numeric_limits<double>::quiet_NaN());
      e->encodeDouble(std::numeric_limits<double>::max());
      e->encodeDouble(std::numeric_limits<double>::min());
      e->encodeFloat(std::numeric_limits<float>::infinity());
      e->encodeFloat(-std::numeric_limits<float>::infinity());
      e->encodeFloat(std::numeric_limits<float>::quiet_NaN());
      e->encodeFloat(std::numeric_limits<float>::max());
      e->encodeFloat(std::numeric_limits<float>::min());
      e->flush();
    }

    {
      std::shared_ptr<InputStream> s2 = memoryInputStream(*s1);
      d->init(*s2);
      REQUIRE(d->decodeDouble() == std::numeric_limits<double>::infinity());
      REQUIRE(d->decodeDouble() == -std::numeric_limits<double>::infinity());
      REQUIRE(boost::math::isnan(d->decodeDouble()));
      REQUIRE(d->decodeDouble() == std::numeric_limits<double>::max());
      REQUIRE(d->decodeDouble() == std::numeric_limits<double>::min());
      REQUIRE(d->decodeFloat() == std::numeric_limits<float>::infinity());
      REQUIRE(d->decodeFloat() == -std::numeric_limits<float>::infinity());
      REQUIRE(boost::math::isnan(d->decodeFloat()));
      REQUIRE(std::abs(d->decodeFloat() - std::numeric_limits<float>::max()) < 0.00011);
      REQUIRE(std::abs(d->decodeFloat() - std::numeric_limits<float>::min()) < 0.00011);
    }
  }

  TEST_CASE("Avro C++ unit tests for codecs: testLimitsBinaryCodec", "[testLimitsBinaryCodec]") {
    testLimits(binaryEncoder(), binaryDecoder());
  }

  TEST_CASE("Avro C++ unit tests for codecs: testLimitsJsonCodec", "[testLimitsJsonCodec]") {
    const char* s = "{ \"type\": \"record\", \"name\": \"r\", \"fields\": ["
      "{ \"name\": \"d1\", \"type\": \"double\" },"
      "{ \"name\": \"d2\", \"type\": \"double\" },"
      "{ \"name\": \"d3\", \"type\": \"double\" },"
      "{ \"name\": \"d4\", \"type\": \"double\" },"
      "{ \"name\": \"d5\", \"type\": \"double\" },"
      "{ \"name\": \"f1\", \"type\": \"float\" },"
      "{ \"name\": \"f2\", \"type\": \"float\" },"
      "{ \"name\": \"f3\", \"type\": \"float\" },"
      "{ \"name\": \"f4\", \"type\": \"float\" },"
      "{ \"name\": \"f5\", \"type\": \"float\" }"
      "]}";
    ValidSchema schema = parsing::makeValidSchema(s);
    testLimits(jsonEncoder(schema), jsonDecoder(schema));
    testLimits(jsonPrettyEncoder(schema), jsonDecoder(schema));
  }

  struct JsonData {
    const char *schema;
    const char *json;
    const char* calls;
    int depth;
  };

  const JsonData jsonData[] = {
    { "{\"type\": \"double\"}", " 10 ", "D", 1},
    { "{\"type\": \"double\"}", " 10.0 ", "D", 1},
    { "{\"type\": \"double\"}", " \"Infinity\"", "D", 1},
    { "{\"type\": \"double\"}", " \"-Infinity\"", "D", 1},
    { "{\"type\": \"double\"}", " \"NaN\"", "D", 1},
    { "{\"type\": \"long\"}", " 10 ", "L", 1},
  };

  static void testJson(const JsonData& data) {
    ValidSchema schema = parsing::makeValidSchema(data.schema);
    EncoderPtr e = jsonEncoder(schema);
  }

  TEST_CASE("Avro C++ unit tests for codecs: testJson", "[testJson]") {
    for (auto& item : avro::jsonData) testJson(item);
  }
}
