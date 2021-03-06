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
#include <sstream>
#include "Compiler.hh"
#include "Types.hh"
#include "Schema.hh"
#include "ValidSchema.hh"
#include "Stream.hh"

#include "json/JsonDom.hh"

using std::string;
using std::map;
using std::vector;
using std::pair;
using std::make_pair;

namespace avro {
  using json::Entity;
  using json::Object;
  using json::Array;
  using json::EntityType;

  typedef map<Name, NodePtr> SymbolTable;

  static NodePtr makePrimitive(const std::string& t) {
    if (t == "null") {
      return NodePtr(new NodePrimitive(Type::AVRO_NULL));
    } else if (t == "boolean") {
      return NodePtr(new NodePrimitive(Type::AVRO_BOOL));
    } else if (t == "int") {
      return NodePtr(new NodePrimitive(Type::AVRO_INT));
    } else if (t == "long") {
      return NodePtr(new NodePrimitive(Type::AVRO_LONG));
    } else if (t == "float") {
      return NodePtr(new NodePrimitive(Type::AVRO_FLOAT));
    } else if (t == "double") {
      return NodePtr(new NodePrimitive(Type::AVRO_DOUBLE));
    } else if (t == "string") {
      return NodePtr(new NodePrimitive(Type::AVRO_STRING));
    } else if (t == "bytes") {
      return NodePtr(new NodePrimitive(Type::AVRO_BYTES));
    } else {
      return NodePtr();
    }
  }

  static NodePtr makeNode(const json::Entity& e, SymbolTable& st, const string& ns);

  template <typename T>
  concepts::SingleAttribute<T> asSingleAttribute(const T& t) {
    concepts::SingleAttribute<T> n;
    n.add(t);
    return n;
  }

  static bool isFullName(const string& s) {
    return s.find('.') != string::npos;
  }

  static Name getName(const string& name, const string& ns) {
    return (isFullName(name)) ? Name(name) : Name(name, ns);
  }

  static NodePtr makeNode(const std::string& t, SymbolTable& st, const string& ns) {
    NodePtr result = makePrimitive(t);
    if (result) {
      return result;
    }
    Name n = getName(t, ns);

    SymbolTable::const_iterator it = st.find(n);
    if (it != st.end()) {
      return NodePtr(new NodeSymbolic(asSingleAttribute(n), it->second));
    }
    throw Exception(boost::format("Unknown type: %1%") % n.fullname());
  }

  const json::Object::const_iterator findField(const Entity& e,
    const Object& m, const string& fieldName) {
    Object::const_iterator it = m.find(fieldName);
    if (it == m.end()) {
      throw Exception(boost::format("Missing Json field \"%1%\": %2%") %
        fieldName % e.toString());
    } else {
      return it;
    }
  }

  template <typename T> void ensureType(const Entity& e, const string& name) {
    if (e.type() != json::type_traits<T>::type()) {
      throw Exception(boost::format("Json field \"%1%\" is not a %2%: %3%") %
        name % json::type_traits<T>::name() % e.toString());
    }
  }

  const string& getStringField(const Entity& e, const Object& m,
    const string& fieldName) {
    Object::const_iterator it = findField(e, m, fieldName);
    ensureType<string>(it->second, fieldName);
    return it->second.stringValue();
  }

  const Array& getArrayField(const Entity& e, const Object& m,
    const string& fieldName) {
    Object::const_iterator it = findField(e, m, fieldName);
    ensureType<Array >(it->second, fieldName);
    return it->second.arrayValue();
  }

  int64_t getLongField(const Entity& e, const Object& m, const string& fieldName) {
    Object::const_iterator it = findField(e, m, fieldName);
    ensureType<int64_t>(it->second, fieldName);
    return it->second.longValue();
  }

  struct Field {
    const string& name;
    const NodePtr schema;
    const GenericDatum defaultValue;

    Field(const string& n, const NodePtr& v, GenericDatum dv) :
    name(n), schema(v), defaultValue(dv) {
    }
  };

  static void assertType(const Entity& e, EntityType et) {
    if (e.type() != et) {
      throw Exception(boost::format("Unexpected type for default value: "
        "Expected %1%, but found %2% in line %3%") %
        json::typeToString(et) % json::typeToString(e.type()) %
        e.line());
    }
  }

  static vector<uint8_t> toBin(const std::string& s) {
    vector<uint8_t> result(s.size());
    if (s.size() > 0) {
      std::copy(s.c_str(), s.c_str() + s.size(), &result[0]);
    }
    return result;
  }

  static GenericDatum makeGenericDatum(NodePtr n,
    const Entity& e, const SymbolTable& st) {
    Type t = n->type();
    if (t == Type::AVRO_SYMBOLIC) {
      n = st.find(n->name())->second;
      t = n->type();
    }
    switch (t) {
      case Type::AVRO_STRING:
        assertType(e, json::etString);
        return GenericDatum(e.stringValue());
      case Type::AVRO_BYTES:
        assertType(e, json::etString);
        return GenericDatum(toBin(e.stringValue()));
      case Type::AVRO_INT:
        assertType(e, json::etLong);
        return GenericDatum(static_cast<int32_t> (e.longValue()));
      case Type::AVRO_LONG:
        assertType(e, json::etLong);
        return GenericDatum(e.longValue());
      case Type::AVRO_FLOAT:
        assertType(e, json::etDouble);
        return GenericDatum(static_cast<float> (e.doubleValue()));
      case Type::AVRO_DOUBLE:
        assertType(e, json::etDouble);
        return GenericDatum(e.doubleValue());
      case Type::AVRO_BOOL:
        assertType(e, json::etBool);
        return GenericDatum(e.boolValue());
      case Type::AVRO_NULL:
        assertType(e, json::etNull);
        return GenericDatum();
      case Type::AVRO_RECORD:
      {
        assertType(e, json::etObject);
        GenericRecord result(n);
        const map<string, Entity>& v = e.objectValue();
        for (size_t i = 0; i < n->leaves(); ++i) {
          map<string, Entity>::const_iterator it = v.find(n->nameAt(i));
          if (it == v.end()) {
            throw Exception(boost::format(
              "No value found in default for %1%") % n->nameAt(i));
          }
          result.setFieldAt(i,
            makeGenericDatum(n->leafAt(i), it->second, st));
        }
        return GenericDatum(n, result);
      }      
      default:
        throw Exception(boost::format("Unknown type: %1%") % t);
    }
    return GenericDatum();
  }

  static Field makeField(const Entity& e, SymbolTable& st, const string& ns) {
    const Object& m = e.objectValue();
    const string& n = getStringField(e, m, "name");
    Object::const_iterator it = findField(e, m, "type");
    map<string, Entity>::const_iterator it2 = m.find("default");
    NodePtr node = makeNode(it->second, st, ns);
    GenericDatum d = (it2 == m.end()) ? GenericDatum() :
      makeGenericDatum(node, it2->second, st);
    return Field(n, node, d);
  }

  static NodePtr makeRecordNode(const Entity& e,
    const Name& name, const Object& m, SymbolTable& st, const string& ns) {
    const Array& v = getArrayField(e, m, "fields");
    concepts::MultiAttribute<string> fieldNames;
    concepts::MultiAttribute<NodePtr> fieldValues;
    vector<GenericDatum> defaultValues;

    for (Array::const_iterator it = v.begin(); it != v.end(); ++it) {
      Field f = makeField(*it, st, ns);
      fieldNames.add(f.name);
      fieldValues.add(f.schema);
      defaultValues.push_back(f.defaultValue);
    }
    return NodePtr(new NodeRecord(asSingleAttribute(name),
      fieldValues, fieldNames, defaultValues));
  }
  
  static Name getName(const Entity& e, const Object& m, const string& ns) {
    const string& name = getStringField(e, m, "name");

    if (isFullName(name)) {
      return Name(name);
    } else {
      Object::const_iterator it = m.find("namespace");
      if (it != m.end()) {
        if (it->second.type() != json::type_traits<string>::type()) {
          throw Exception(boost::format(
            "Json field \"%1%\" is not a %2%: %3%") %
            "namespace" % json::type_traits<string>::name() %
            it->second.toString());
        }
        Name result = Name(name, it->second.stringValue());
        return result;
      }
      return Name(name, ns);
    }
  }

  static NodePtr makeNode(const Entity& e, const Object& m,
    SymbolTable& st, const string& ns) {
    const string& type = getStringField(e, m, "type");
    if (NodePtr result = makePrimitive(type)) {
      return result;
    } else if (type == "record" || type == "error" ||
      type == "enum" || type == "fixed") {
      Name nm = getName(e, m, ns);
      NodePtr result;
      if (type == "record" || type == "error") {
        result = NodePtr(new NodeRecord());
        st[nm] = result;
        NodePtr r = makeRecordNode(e, nm, m, st, nm.ns());
        (std::dynamic_pointer_cast<NodeRecord>(r))->swap(
          *std::dynamic_pointer_cast<NodeRecord>(result));
      }
      return result;
    }
    throw Exception(boost::format("Unknown type definition: %1%")
      % e.toString());
  }

  static NodePtr makeNode(const json::Entity& e, SymbolTable& st, const string& ns) {
    switch (e.type()) {
      case json::etString:
        return makeNode(e.stringValue(), st, ns);
      case json::etObject:
        return makeNode(e, e.objectValue(), st, ns);
      default:
        throw Exception(boost::format("Invalid Avro type: %1%") % e.toString());
    }
  }

  ValidSchema compileJsonSchemaFromStream(InputStream& is) {
    json::Entity e = json::loadEntity(is);
    SymbolTable st;
    NodePtr n = makeNode(e, st, "");
    return ValidSchema(n);
  }

  ValidSchema compileJsonSchemaFromFile(const char* filename) {
    std::shared_ptr<InputStream> s = fileInputStream(filename);
    return compileJsonSchemaFromStream(*s);
  }

  ValidSchema compileJsonSchemaFromMemory(const uint8_t* input, size_t len) {
    return compileJsonSchemaFromStream(*memoryInputStream(input, len));
  }

  ValidSchema compileJsonSchemaFromString(const char* input) {
    return compileJsonSchemaFromMemory(reinterpret_cast<const uint8_t*> (input),
      ::strlen(input));
  }

  ValidSchema compileJsonSchemaFromString(const std::string& input) {
    return compileJsonSchemaFromMemory(
      reinterpret_cast<const uint8_t*> (&input[0]), input.size());
  }

  static ValidSchema compile(std::istream& is) {
    std::shared_ptr<InputStream> in = istreamInputStream(is);
    return compileJsonSchemaFromStream(*in);
  }

  void compileJsonSchema(std::istream &is, ValidSchema &schema) {
    if (!is.good()) {
      throw Exception("Input stream is not good");
    }

    schema = compile(is);
  }

  bool compileJsonSchema(std::istream &is, ValidSchema &schema, std::string &error) {
    try {
      compileJsonSchema(is, schema);
      return true;
    } catch (const Exception &e) {
      error = e.what();
      return false;
    }

  }

}
