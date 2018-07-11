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


#include "NodeImpl.hh"

namespace avro {

  SchemaResolution NodePrimitive::resolve(const Node &reader) const {
    if (type() == reader.type()) {
      return SchemaResolution::MATCH;
    }

    if ((type() == Type::AVRO_INT) && reader.type() == Type::AVRO_LONG) {
      return SchemaResolution::PROMOTABLE_TO_LONG;
    }

    if ((type() == Type::AVRO_INT || type() == Type::AVRO_LONG) && reader.type() == Type::AVRO_FLOAT) {
      return SchemaResolution::PROMOTABLE_TO_FLOAT;
    }

    if ((type() == Type::AVRO_INT || type() == Type::AVRO_LONG || type() == Type::AVRO_FLOAT) && reader.type() == Type::AVRO_DOUBLE) {
      return SchemaResolution::PROMOTABLE_TO_DOUBLE;
    }

    return furtherResolution(reader);
  }

  SchemaResolution NodeRecord::resolve(const Node &reader) const {
    if (reader.type() == Type::AVRO_RECORD) {
      if (name() == reader.name()) {
        return SchemaResolution::MATCH;
      }
    }
    return furtherResolution(reader);
  }

  SchemaResolution NodeEnum::resolve(const Node &reader) const {
    if (reader.type() == Type::AVRO_ENUM) {
      return (name() == reader.name()) ? SchemaResolution::MATCH : SchemaResolution::NO_MATCH;
    }
    return furtherResolution(reader);
  }

  SchemaResolution NodeArray::resolve(const Node &reader) const {
    if (reader.type() == Type::AVRO_ARRAY) {
      const NodePtr &arrayType = leafAt(0);
      return arrayType->resolve(*reader.leafAt(0));
    }
    return furtherResolution(reader);
  }

  SchemaResolution NodeMap::resolve(const Node &reader) const {
    if (reader.type() == Type::AVRO_MAP) {
      const NodePtr &mapType = leafAt(1);
      return mapType->resolve(*reader.leafAt(1));
    }
    return furtherResolution(reader);
  }

  SchemaResolution NodeUnion::resolve(const Node &reader) const {

    // If the writer is union, resolution only needs to occur when the selected
    // type of the writer is known, so this function is not very helpful.
    //
    // In this case, this function returns if there is a possible match given
    // any writer type, so just search type by type returning the best match
    // found.

    SchemaResolution match = SchemaResolution::NO_MATCH;
    for (size_t i = 0; i < leaves(); ++i) {
      const NodePtr &node = leafAt(i);
      SchemaResolution thisMatch = node->resolve(reader);
      if (thisMatch == SchemaResolution::MATCH) {
        match = thisMatch;
        break;
      }
      if (match == SchemaResolution::NO_MATCH) {
        match = thisMatch;
      }
    }
    return match;
  }

  SchemaResolution NodeFixed::resolve(const Node &reader) const {
    if (reader.type() == Type::AVRO_FIXED) {
      return (
        (reader.fixedSize() == fixedSize()) &&
        (reader.name() == name())
        ) ?
        SchemaResolution::MATCH : SchemaResolution::NO_MATCH;
    }
    return furtherResolution(reader);
  }

  SchemaResolution NodeSymbolic::resolve(const Node &reader) const {
    const NodePtr &node = leafAt(0);
    return node->resolve(reader);
  }

  /* Wrap an indentation in a struct for ostream operator<< */
  struct indent {

    indent(int depth) :
    d(depth) {
    }
    int d;
  };

  /* ostream operator for indent*/
  std::ostream& operator<<(std::ostream &os, indent x) {
    static const std::string spaces("    ");
    while (x.d--) {
      os << spaces;
    }
    return os;
  }

  void NodePrimitive::printJson(std::ostream &os, int depth) const {
    os << '\"' << type() << '\"';
  }

  void NodeSymbolic::printJson(std::ostream &os, int depth) const {
    os << '\"' << nameAttribute_.get() << '\"';
  }

  static void printName(std::ostream& os, const Name& n, int depth) {
    if (!n.ns().empty()) {
      os << indent(depth) << "\"namespace\": \"" << n.ns() << "\",\n";
    }
    os << indent(depth) << "\"name\": \"" << n.simpleName() << "\",\n";
  }

  void NodeRecord::printJson(std::ostream &os, int depth) const {
    os << "{\n";
    os << indent(++depth) << "\"type\": \"record\",\n";
    printName(os, nameAttribute_.get(), depth);
    os << indent(depth) << "\"fields\": [";

    int fields = leafAttributes_.size();
    ++depth;
    for (int i = 0; i < fields; ++i) {
      if (i > 0) {
        os << ',';
      }
      os << '\n' << indent(depth) << "{\n";
      os << indent(++depth) << "\"name\": \"" << leafNameAttributes_.get(i) << "\",\n";
      os << indent(depth) << "\"type\": ";
      leafAttributes_.get(i)->printJson(os, depth);
      os << '\n';
      os << indent(--depth) << '}';
    }
    os << '\n' << indent(--depth) << "]\n";
    os << indent(--depth) << '}';
  }

  void NodeEnum::printJson(std::ostream &os, int depth) const {
    os << "{\n";
    os << indent(++depth) << "\"type\": \"enum\",\n";
    printName(os, nameAttribute_.get(), depth);
    os << indent(depth) << "\"symbols\": [\n";

    int names = leafNameAttributes_.size();
    ++depth;
    for (int i = 0; i < names; ++i) {
      if (i > 0) {
        os << ",\n";
      }
      os << indent(depth) << '\"' << leafNameAttributes_.get(i) << '\"';
    }
    os << '\n';
    os << indent(--depth) << "]\n";
    os << indent(--depth) << '}';
  }

  void NodeArray::printJson(std::ostream &os, int depth) const {
    os << "{\n";
    os << indent(depth + 1) << "\"type\": \"array\",\n";
    os << indent(depth + 1) << "\"items\": ";
    leafAttributes_.get()->printJson(os, depth + 1);
    os << '\n';
    os << indent(depth) << '}';
  }

  void NodeMap::printJson(std::ostream &os, int depth) const {
    os << "{\n";
    os << indent(depth + 1) << "\"type\": \"map\",\n";
    os << indent(depth + 1) << "\"values\": ";
    leafAttributes_.get(1)->printJson(os, depth + 1);
    os << '\n';
    os << indent(depth) << '}';
  }

  void NodeUnion::printJson(std::ostream &os, int depth) const {
    os << "[\n";
    int fields = leafAttributes_.size();
    ++depth;
    for (int i = 0; i < fields; ++i) {
      if (i > 0) {
        os << ",\n";
      }
      os << indent(depth);
      leafAttributes_.get(i)->printJson(os, depth);
    }
    os << '\n';
    os << indent(--depth) << ']';
  }

  void NodeFixed::printJson(std::ostream &os, int depth) const {
    os << "{\n";
    os << indent(++depth) << "\"type\": \"fixed\",\n";
    printName(os, nameAttribute_.get(), depth);
    os << indent(depth) << "\"size\": " << sizeAttribute_.get() << "\n";
    os << indent(--depth) << '}';
  }

}
