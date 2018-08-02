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

#include "Generic.hh"
#include <sstream>

namespace avro {

  using std::string;
  using std::vector;
  using std::ostringstream;

  typedef vector<uint8_t> bytes;

  void GenericContainer::assertType(const NodePtr& schema, Type type) {
    if (schema->type() != type) {
      throw Exception(boost::format("Schema type %1 expected %2") %
        toString(schema->type()) % toString(type));
    }
  }

  GenericReader::GenericReader(const ValidSchema& s, const DecoderPtr& decoder) :
  schema_(s), isResolving_(dynamic_cast<ResolvingDecoder*> (&(*decoder)) != 0),
  decoder_(decoder) {
  }

  GenericReader::GenericReader(const ValidSchema& writerSchema,
    const ValidSchema& readerSchema, const DecoderPtr& decoder) :
  schema_(readerSchema),
  isResolving_(true),
  decoder_(resolvingDecoder(writerSchema, readerSchema, decoder)) {
  }

  void GenericReader::read(GenericDatum& datum) const {
    datum = GenericDatum(schema_.root());
    read(datum, *decoder_, isResolving_);
  }

  void GenericReader::read(GenericDatum& datum, Decoder& d, bool isResolving) {
    switch (datum.type()) {
      case Type::AVRO_NULL:
        d.decodeNull();
        break;
      case Type::AVRO_BOOL:
        datum.value<bool>() = d.decodeBool();
        break;
      case Type::AVRO_INT:
        datum.value<int32_t>() = d.decodeInt();
        break;
      case Type::AVRO_LONG:
        datum.value<int64_t>() = d.decodeLong();
        break;
      case Type::AVRO_FLOAT:
        datum.value<float>() = d.decodeFloat();
        break;
      case Type::AVRO_DOUBLE:
        datum.value<double>() = d.decodeDouble();
        break;
      case Type::AVRO_STRING:
        d.decodeString(datum.value<string>());
        break;
      case Type::AVRO_BYTES:
        d.decodeBytes(datum.value<bytes>());
        break;
      case Type::AVRO_RECORD:
      {
        GenericRecord& r = datum.value<GenericRecord>();
        size_t c = r.schema()->leaves();
        if (isResolving) {
          std::vector<size_t> fo =
            static_cast<ResolvingDecoder&> (d).fieldOrder();
          for (size_t i = 0; i < c; ++i) {
            read(r.fieldAt(fo[i]), d, isResolving);
          }
        } else {
          for (size_t i = 0; i < c; ++i) {
            read(r.fieldAt(i), d, isResolving);
          }
        }
      }
        break;
      default:
        throw Exception(boost::format("Unknown schema type %1%") %
          toString(datum.type()));
    }
  }

  void GenericReader::read(Decoder& d, GenericDatum& g, const ValidSchema& s) {
    g = GenericDatum(s);
    read(d, g);
  }

  void GenericReader::read(Decoder& d, GenericDatum& g) {
    read(g, d, dynamic_cast<ResolvingDecoder*> (&d) != 0);
  }

  GenericWriter::GenericWriter(const ValidSchema& s, const EncoderPtr& encoder) :
  schema_(s), encoder_(encoder) {
  }

  void GenericWriter::write(const GenericDatum& datum) const {
    write(datum, *encoder_);
  }

  void GenericWriter::write(const GenericDatum& datum, Encoder& e) {
    switch (datum.type()) {
      case Type::AVRO_NULL:
        e.encodeNull();
        break;
      case Type::AVRO_BOOL:
        e.encodeBool(datum.value<bool>());
        break;
      case Type::AVRO_INT:
        e.encodeInt(datum.value<int32_t>());
        break;
      case Type::AVRO_LONG:
        e.encodeLong(datum.value<int64_t>());
        break;
      case Type::AVRO_FLOAT:
        e.encodeFloat(datum.value<float>());
        break;
      case Type::AVRO_DOUBLE:
        e.encodeDouble(datum.value<double>());
        break;
      case Type::AVRO_STRING:
        e.encodeString(datum.value<string>());
        break;
      case Type::AVRO_BYTES:
        e.encodeBytes(datum.value<bytes>());
        break;
      case Type::AVRO_RECORD:
      {
        const GenericRecord& r = datum.value<GenericRecord>();
        size_t c = r.schema()->leaves();
        for (size_t i = 0; i < c; ++i) {
          write(r.fieldAt(i), e);
        }
      }
        break;
      default:
        throw Exception(boost::format("Unknown schema type %1%") %
          toString(datum.type()));
    }
  }

  void GenericWriter::write(Encoder& e, const GenericDatum& g) {
    write(g, e);
  }

}
