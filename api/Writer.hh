/*
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

#ifndef avro_Writer_hh__
#define avro_Writer_hh__

#include <boost/noncopyable.hpp>

#include "buffer/Buffer.hh"
#include "Zigzag.hh"
#include "Types.hh"
#include "Validator.hh"

namespace avro {

  /* Class for writing avro data to a stream.*/
  template<class ValidatorType>
  class WriterImpl {
  public:

    WriterImpl(const WriterImpl&) = delete;
    const WriterImpl& operator=(const WriterImpl&) = delete;

    WriterImpl() { }

    explicit WriterImpl(const ValidSchema &schema) : validator_(schema) { }

    void writeValue(const Null &) {
      validator_.checkTypeExpected(Type::AVRO_NULL);
    }

    void writeValue(bool val) {
      validator_.checkTypeExpected(Type::AVRO_BOOL);
      int8_t byte = (val != 0);
      buffer_.writeTo(byte);
    }

    void writeValue(int32_t val) {
      validator_.checkTypeExpected(Type::AVRO_INT);
      boost::array<uint8_t, 5> bytes;
      size_t size = encodeInt32(val, bytes);
      buffer_.writeTo(reinterpret_cast<const char *> (bytes.data()), size);
    }

    void writeValue(int64_t val) {
      validator_.checkTypeExpected(Type::AVRO_LONG);
      putLong(val);
    }

    void writeValue(float val) {
      validator_.checkTypeExpected(Type::AVRO_FLOAT);

      union {
        float f;
        int32_t i;
      } v;

      v.f = val;
      buffer_.writeTo(v.i);
    }

    void writeValue(double val) {
      validator_.checkTypeExpected(Type::AVRO_DOUBLE);

      union {
        double d;
        int64_t i;
      } v;

      v.d = val;
      buffer_.writeTo(v.i);
    }

    void writeValue(const std::string &val) {
      validator_.checkTypeExpected(Type::AVRO_STRING);
      putBytes(val.c_str(), val.size());
    }

    void writeBytes(const void *val, size_t size) {
      validator_.checkTypeExpected(Type::AVRO_BYTES);
      putBytes(val, size);
    }

    void writeRecord() {
      validator_.checkTypeExpected(Type::AVRO_RECORD);
      validator_.checkTypeExpected(Type::AVRO_LONG);
      validator_.setCount(1);
    }

    void writeRecordEnd() {
      validator_.checkTypeExpected(Type::AVRO_RECORD);
      validator_.checkTypeExpected(Type::AVRO_LONG);
      validator_.setCount(0);
    }

    InputBuffer buffer() const {
      return buffer_;
    }

  private:

    void putLong(int64_t val) {
      boost::array<uint8_t, 10> bytes;
      size_t size = encodeInt64(val, bytes);
      buffer_.writeTo(reinterpret_cast<const char *> (bytes.data()), size);
    }

    void putBytes(const void *val, size_t size) {
      putLong(size);
      buffer_.writeTo(reinterpret_cast<const char *> (val), size);
    }

    void writeCount(int64_t count) {
      validator_.checkTypeExpected(Type::AVRO_LONG);
      validator_.setCount(count);
      putLong(count);
    }

    ValidatorType validator_;
    OutputBuffer buffer_;

  };

  typedef WriterImpl<NullValidator> Writer;
  typedef WriterImpl<Validator> ValidatingWriter;

}

#endif
