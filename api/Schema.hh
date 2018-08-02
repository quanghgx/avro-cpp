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

#ifndef avro_Schema_hh__ 
#define avro_Schema_hh__ 

#include "NodeImpl.hh"

/// \file
///
/// Schemas for representing all the avro types.  The compound schema objects
/// allow composition from other schemas.
/**/
namespace avro {

  /* The root Schema object is a base class.  Nobody constructs this class directly.*/
  class Schema {
  public:

    virtual ~Schema();

    Type type() const {
      return node_->type();
    }

    const NodePtr &root() const {
      return node_;
    }

    NodePtr &root() {
      return node_;
    }

  protected:
    Schema();
    explicit Schema(const NodePtr &node);
    explicit Schema(Node *node);

    NodePtr node_;
  };

  class NullSchema : public Schema {
  public:

    NullSchema() : Schema(new NodePrimitive(Type::AVRO_NULL)) { }
  };

  class BoolSchema : public Schema {
  public:

    BoolSchema() : Schema(new NodePrimitive(Type::AVRO_BOOL)) { }
  };

  class IntSchema : public Schema {
  public:

    IntSchema() : Schema(new NodePrimitive(Type::AVRO_INT)) { }
  };

  class LongSchema : public Schema {
  public:

    LongSchema() : Schema(new NodePrimitive(Type::AVRO_LONG)) { }
  };

  class FloatSchema : public Schema {
  public:

    FloatSchema() : Schema(new NodePrimitive(Type::AVRO_FLOAT)) { }
  };

  class DoubleSchema : public Schema {
  public:

    DoubleSchema() : Schema(new NodePrimitive(Type::AVRO_DOUBLE)) { }
  };

  class StringSchema : public Schema {
  public:

    StringSchema() : Schema(new NodePrimitive(Type::AVRO_STRING)) { }
  };

  class BytesSchema : public Schema {
  public:

    BytesSchema() : Schema(new NodePrimitive(Type::AVRO_BYTES)) { }
  };

  class RecordSchema : public Schema {
  public:
    RecordSchema(const std::string &name);
    void addField(const std::string &name, const Schema &fieldSchema);
  };

  class SymbolicSchema : public Schema {
  public:
    SymbolicSchema(const Name& name, const NodePtr& link);
  };
}

#endif
