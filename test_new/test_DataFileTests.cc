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
#include <sstream>
#include <memory>
#include "DataFile.hh"
#include "Generic.hh"
#include "Stream.hh"
#include "Compiler.hh"
#include <boost/filesystem.hpp>

using std::string;
using std::pair;
using std::vector;
using std::map;
using std::istringstream;
using std::ostringstream;
using std::shared_ptr;
using avro::ValidSchema;
using avro::GenericDatum;
using avro::GenericRecord;

const int count = 1000;

template <typename T>
struct Complex {
  T re;
  T im;

  Complex() : re(0), im(0) {
  }

  Complex(T r, T i) : re(r), im(i) {
  }
};

struct Integer {
  int64_t re;

  Integer() : re(0) {
  }

  Integer(int64_t r) : re(r) {
  }
};

typedef Complex<int64_t> ComplexInteger;
typedef Complex<double> ComplexDouble;

struct Double {
  double re;

  Double() : re(0) {
  }

  Double(double r) : re(r) {
  }
};

namespace avro {

  template <typename T> struct codec_traits<Complex<T> > {

    static void encode(Encoder& e, const Complex<T>& c) {
      avro::encode(e, c.re);
      avro::encode(e, c.im);
    }

    static void decode(Decoder& d, Complex<T>& c) {
      avro::decode(d, c.re);
      avro::decode(d, c.im);
    }
  };

  template <> struct codec_traits<Integer> {

    static void decode(Decoder& d, Integer& c) {
      avro::decode(d, c.re);
    }
  };

  template <> struct codec_traits<Double> {

    static void decode(Decoder& d, Double& c) {
      avro::decode(d, c.re);
    }
  };

  template<> struct codec_traits<uint32_t> {

    static void encode(Encoder& e, const uint32_t& v) {
      e.encodeFixed((uint8_t *) & v, sizeof (uint32_t));
    }

    static void decode(Decoder& d, uint32_t& v) {
      std::vector <uint8_t> value;
      d.decodeFixed(sizeof (uint32_t), value);
      memcpy(&v, &(value[0]), sizeof (uint32_t));
    }
  };

}

static ValidSchema makeValidSchema(const char* schema) {
  istringstream iss(schema);
  ValidSchema vs;
  compileJsonSchema(iss, vs);
  return ValidSchema(vs);
}

static const char sch[] = "{\"type\": \"record\","
  "\"name\":\"ComplexInteger\", \"fields\": ["
  "{\"name\":\"re\", \"type\":\"long\"},"
  "{\"name\":\"im\", \"type\":\"long\"}"
  "]}";
static const char isch[] = "{\"type\": \"record\","
  "\"name\":\"ComplexInteger\", \"fields\": ["
  "{\"name\":\"re\", \"type\":\"long\"}"
  "]}";
static const char dsch[] = "{\"type\": \"record\","
  "\"name\":\"ComplexDouble\", \"fields\": ["
  "{\"name\":\"re\", \"type\":\"double\"},"
  "{\"name\":\"im\", \"type\":\"double\"}"
  "]}";
static const char dblsch[] = "{\"type\": \"record\","
  "\"name\":\"ComplexDouble\", \"fields\": ["
  "{\"name\":\"re\", \"type\":\"double\"}"
  "]}";
static const char fsch[] = "{\"type\": \"fixed\","
  "\"name\":\"Fixed_32\", \"size\":4}";

string toString(const ValidSchema& s) {
  ostringstream oss;
  s.toJson(oss);
  return oss.str();
}

class DataFileTest {
  const char* filename;
  const ValidSchema writerSchema;
  const ValidSchema readerSchema;

public:

  DataFileTest(const char* f, const char* wsch, const char* rsch) :
  filename(f), writerSchema(makeValidSchema(wsch)),
  readerSchema(makeValidSchema(rsch)) {
  }

  typedef pair<ValidSchema, GenericDatum> Pair;

  void testCleanup() {
    REQUIRE(boost::filesystem::remove(filename));
  }

  void testWrite() {
    avro::DataFileWriter<ComplexInteger> df(filename, writerSchema, 100);
    int64_t re = 3;
    int64_t im = 5;
    for (int i = 0; i < count; ++i, re *= im, im += 3) {
      ComplexInteger c(re, im);
      df.write(c);
    }
    df.close();
  }

  void testWriteGeneric() {
    avro::DataFileWriter<Pair> df(filename, writerSchema, 100);
    int64_t re = 3;
    int64_t im = 5;
    Pair p(writerSchema, GenericDatum());

    GenericDatum& c = p.second;
    c = GenericDatum(writerSchema.root());
    GenericRecord& r = c.value<GenericRecord>();

    for (int i = 0; i < count; ++i, re *= im, im += 3) {
      r.fieldAt(0) = re;
      r.fieldAt(1) = im;
      df.write(p);
    }
    df.close();
  }

  void testWriteGenericByName() {
    avro::DataFileWriter<Pair> df(filename, writerSchema, 100);
    int64_t re = 3;
    int64_t im = 5;
    Pair p(writerSchema, GenericDatum());

    GenericDatum& c = p.second;
    c = GenericDatum(writerSchema.root());
    GenericRecord& r = c.value<GenericRecord>();

    for (int i = 0; i < count; ++i, re *= im, im += 3) {
      r.field("re") = re;
      r.field("im") = im;
      df.write(p);
    }
    df.close();
  }

  void testWriteDouble() {
    avro::DataFileWriter<ComplexDouble> df(filename, writerSchema, 100);
    double re = 3.0;
    double im = 5.0;
    for (int i = 0; i < count; ++i, re += im - 0.7, im += 3.1) {
      ComplexDouble c(re, im);
      df.write(c);
    }
    df.close();
  }

  void testTruncate() {
    testWriteDouble();
    uintmax_t size = boost::filesystem::file_size(filename);
    {
      avro::DataFileWriter<Pair> df(filename, writerSchema, 100);
      df.close();
    }
    uintmax_t new_size = boost::filesystem::file_size(filename);
    REQUIRE(size > new_size);
  }

  void testReadFull() {
    avro::DataFileReader<ComplexInteger> df(filename, writerSchema);
    int i = 0;
    ComplexInteger ci;
    int64_t re = 3;
    int64_t im = 5;
    while (df.read(ci)) {
      REQUIRE(ci.re == re);
      REQUIRE(ci.im == im);
      re *= im;
      im += 3;
      ++i;
    }
    REQUIRE(i == count);
  }

  void testReadProjection() {
    avro::DataFileReader<Integer> df(filename, readerSchema);
    int i = 0;
    Integer integer;
    int64_t re = 3;
    int64_t im = 5;
    while (df.read(integer)) {
      REQUIRE(integer.re == re);
      re *= im;
      im += 3;
      ++i;
    }
    REQUIRE(i == count);
  }

  void testReaderGeneric() {
    avro::DataFileReader<Pair> df(filename, writerSchema);
    int i = 0;
    Pair p(writerSchema, GenericDatum());
    int64_t re = 3;
    int64_t im = 5;

    const GenericDatum& ci = p.second;
    while (df.read(p)) {
      REQUIRE(ci.type() == avro::AVRO_RECORD);
      const GenericRecord& r = ci.value<GenericRecord>();
      const size_t n = 2;
      REQUIRE(r.fieldCount() == n);
      const GenericDatum& f0 = r.fieldAt(0);
      REQUIRE(f0.type() == avro::AVRO_LONG);
      REQUIRE(f0.value<int64_t>() == re);

      const GenericDatum& f1 = r.fieldAt(1);
      REQUIRE(f1.type() == avro::AVRO_LONG);
      REQUIRE(f1.value<int64_t>() == im);
      re *= im;
      im += 3;
      ++i;
    }
    REQUIRE(i == count);
  }

  void testReaderGenericByName() {
    avro::DataFileReader<Pair> df(filename, writerSchema);
    int i = 0;
    Pair p(writerSchema, GenericDatum());
    int64_t re = 3;
    int64_t im = 5;

    const GenericDatum& ci = p.second;
    while (df.read(p)) {
      REQUIRE(ci.type() == avro::AVRO_RECORD);
      const GenericRecord& r = ci.value<GenericRecord>();
      const size_t n = 2;
      REQUIRE(r.fieldCount() == n);
      const GenericDatum& f0 = r.field("re");
      REQUIRE(f0.type() == avro::AVRO_LONG);
      REQUIRE(f0.value<int64_t>() == re);

      const GenericDatum& f1 = r.field("im");
      REQUIRE(f1.type() == avro::AVRO_LONG);
      REQUIRE(f1.value<int64_t>() == im);
      re *= im;
      im += 3;
      ++i;
    }
    REQUIRE(i == count);
  }

  void testReaderGenericProjection() {
    avro::DataFileReader<Pair> df(filename, readerSchema);
    int i = 0;
    Pair p(readerSchema, GenericDatum());
    int64_t re = 3;
    int64_t im = 5;

    const GenericDatum& ci = p.second;
    while (df.read(p)) {
      REQUIRE(ci.type() == avro::AVRO_RECORD);
      const GenericRecord& r = ci.value<GenericRecord>();
      const size_t n = 1;
      REQUIRE(r.fieldCount() == n);
      const GenericDatum& f0 = r.fieldAt(0);
      REQUIRE(f0.type() == avro::AVRO_LONG);
      REQUIRE(f0.value<int64_t>() == re);

      re *= im;
      im += 3;
      ++i;
    }
    REQUIRE(i == count);
  }

  void testReadDouble() {
    avro::DataFileReader<ComplexDouble> df(filename, writerSchema);
    int i = 0;
    ComplexDouble ci;
    double re = 3.0;
    double im = 5.0;
    while (df.read(ci)) {
      REQUIRE(std::abs(ci.re - re) < 0.0001);
      REQUIRE(std::abs(ci.im - im) < 0.0001);
      re += (im - 0.7);
      im += 3.1;
      ++i;
    }
    REQUIRE(i == count);
  }

  /*Constructs the DataFileReader in two steps.*/
  void testReadDoubleTwoStep() {
    shared_ptr<avro::DataFileReaderBase> base(new avro::DataFileReaderBase(filename));
    avro::DataFileReader<ComplexDouble> df(base);
    REQUIRE(toString(writerSchema) == toString(df.readerSchema()));
    REQUIRE(toString(writerSchema) == toString(df.dataSchema()));
    int i = 0;
    ComplexDouble ci;
    double re = 3.0;
    double im = 5.0;
    while (df.read(ci)) {
      REQUIRE(std::abs(ci.re - re) < 0.0001);
      REQUIRE(std::abs(ci.im - im) < 0.0001);
      re += (im - 0.7);
      im += 3.1;
      ++i;
    }
    REQUIRE(i == count);
  }

  /**
   * Constructs the DataFileReader in two steps using a different
   * reader schema.
   */
  void testReadDoubleTwoStepProject() {
    shared_ptr<avro::DataFileReaderBase>
      base(new avro::DataFileReaderBase(filename));
    avro::DataFileReader<Double> df(base, readerSchema);

    REQUIRE(toString(readerSchema) == toString(df.readerSchema()));
    REQUIRE(toString(writerSchema) == toString(df.dataSchema()));
    int i = 0;
    Double ci;
    double re = 3.0;
    double im = 5.0;
    while (df.read(ci)) {
      REQUIRE(std::abs(ci.re - re) < 0.0001);
      re += (im - 0.7);
      im += 3.1;
      ++i;
    }
    REQUIRE(i == count);
  }

  /*Test writing DataFiles into other streams operations.*/
  void testZip() {
    const size_t number_of_objects = 100;
    // first create a large file
    ValidSchema dschema = avro::compileJsonSchemaFromString(sch);
    {
      avro::DataFileWriter<ComplexInteger> writer(
        filename, dschema, 16 * 1024, avro::DEFLATE_CODEC);

      for (size_t i = 0; i < number_of_objects; ++i) {
        ComplexInteger d;
        d.re = i;
        d.im = 2 * i;
        writer.write(d);
      }
    }
    {
      avro::DataFileReader<ComplexInteger> reader(filename, dschema);
      std::vector<int> found;
      ComplexInteger record;
      while (reader.read(record)) {
        found.push_back(record.re);
      }
      REQUIRE(found.size() == number_of_objects);
      for (unsigned int i = 0; i < found.size(); ++i) {
        REQUIRE(found[i] == i);
      }
    }
  }

  void testSchemaReadWrite() {
    uint32_t a = 42;
    {
      avro::DataFileWriter<uint32_t> df(filename, writerSchema);
      df.write(a);
    }

    {
      avro::DataFileReader<uint32_t> df(filename);
      uint32_t b;
      df.read(b);
      REQUIRE(b == a);
    }
  }
};

void addReaderTests(const shared_ptr<DataFileTest>& t) {
  t->testReadFull();
  t->testReadProjection();
  t->testReaderGeneric();
  t->testReaderGenericByName();
  t->testReaderGenericProjection();
  t->testCleanup();
}

TEST_CASE("DataFile tests", "[DataFile tests]") {
  shared_ptr<DataFileTest> t1(new DataFileTest("test1.df", sch, isch));
  t1->testWrite();
  addReaderTests(t1);

  shared_ptr<DataFileTest> t2(new DataFileTest("test2.df", sch, isch));
  t2->testWriteGeneric();
  addReaderTests(t2);

  shared_ptr<DataFileTest> t3(new DataFileTest("test3.df", dsch, dblsch));
  t3->testWriteDouble();
  t3->testReadDouble();
  t3->testReadDoubleTwoStep();
  t3->testReadDoubleTwoStepProject();
  t3->testCleanup();

  shared_ptr<DataFileTest> t4(new DataFileTest("test4.df", dsch, dblsch));
  t4->testTruncate();
  t4->testCleanup();

  shared_ptr<DataFileTest> t5(new DataFileTest("test5.df", sch, isch));
  t5->testWriteGenericByName();
  addReaderTests(t5);

  shared_ptr<DataFileTest> t6(new DataFileTest("test6.df", dsch, dblsch));
  t6->testZip();
  shared_ptr<DataFileTest> t8(new DataFileTest("test8.df", dsch, dblsch));

  shared_ptr<DataFileTest> t7(new DataFileTest("test7.df", fsch, fsch));
  t7->testSchemaReadWrite();
  t7->testCleanup();
}