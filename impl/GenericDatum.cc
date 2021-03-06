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

#include "GenericDatum.hh"
#include "NodeImpl.hh"

using std::string;
using std::vector;

namespace avro {

  GenericDatum::GenericDatum(const ValidSchema& schema) :
  type_(schema.root()->type()) {
    init(schema.root());
  }

  GenericDatum::GenericDatum(const NodePtr& schema) : type_(schema->type()) {
    init(schema);
  }

  void GenericDatum::init(const NodePtr& schema) {
    NodePtr sc = schema;
    if (type_ == Type::AVRO_SYMBOLIC) {
      sc = resolveSymbol(schema);
      type_ = sc->type();
    }
    switch (type_) {
      case Type::AVRO_NULL:
        break;
      case Type::AVRO_BOOL:
        value_ = bool();
        break;
      case Type::AVRO_INT:
        value_ = int32_t();
        break;
      case Type::AVRO_LONG:
        value_ = int64_t();
        break;
      case Type::AVRO_FLOAT:
        value_ = float();
        break;
      case Type::AVRO_DOUBLE:
        value_ = double();
        break;
      case Type::AVRO_STRING:
        value_ = string();
        break;
      case Type::AVRO_BYTES:
        value_ = vector<uint8_t>();
        break;
      case Type::AVRO_RECORD:
        value_ = GenericRecord(sc);
        break;
      default:
        throw Exception(boost::format("Unknown schema type %1%") %
          toString(type_));
    }
  }

  GenericRecord::GenericRecord(const NodePtr& schema) :
  GenericContainer(Type::AVRO_RECORD, schema) {
    fields_.resize(schema->leaves());
    for (size_t i = 0; i < schema->leaves(); ++i) {
      fields_[i] = GenericDatum(schema->leafAt(i));
    }
  }

}
