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

#ifndef avro_GenericDatum_hh__
#define avro_GenericDatum_hh__

#include <cstdint>
#include <vector>
#include <map>
#include <string>

#include <boost/any.hpp>

#include "Node.hh"
#include "ValidSchema.hh"

namespace avro {

  /**
   * Generic datum which can hold any Avro type. The datum has a type
   * and a value. The type is one of the Avro data types. The C++ type for
   * value corresponds to the Avro type.
   * \li An Avro <tt>null</tt> corresponds to no C++ type. It is illegal to
   * to try to access values for <tt>null</tt>.
   * \li Avro <tt>boolean</tt> maps to C++ <tt>bool</tt>
   * \li Avro <tt>int</tt> maps to C++ <tt>int32_t</tt>.
   * \li Avro <tt>long</tt> maps to C++ <tt>int64_t</tt>.
   * \li Avro <tt>float</tt> maps to C++ <tt>float</tt>.
   * \li Avro <tt>double</tt> maps to C++ <tt>double</tt>.
   * \li Avro <tt>string</tt> maps to C++ <tt>std::string</tt>.
   * \li Avro <tt>bytes</tt> maps to C++ <tt>std::vector&lt;uint_t&gt;</tt>.   
   * object should have the C++ type corresponing to one of the constituent
   * types of the union.
   *
   */
  class GenericDatum {
    Type type_;
    boost::any value_;

    GenericDatum(Type t) : type_(t) { }

    template <typename T>
    GenericDatum(Type t, const T& v) : type_(t), value_(v) { }

    void init(const NodePtr& schema);
  public:

    /* The avro data type this datum holds.*/
    Type type() const;

    /* Returns the value held by this datum. T The type for the value. This must correspond to the avro type returned by type().*/
    template<typename T> const T& value() const;

    /* Returns the reference to the value held by this datum, which can be used to change the contents. Please note that only value can be 
       changed, the data type of the value held cannot be changed. T The type for the value. This must correspond to the avro type returned 
       by type().*/
    template<typename T> T& value();

    /* Makes a new AVRO_NULL datum.*/
    GenericDatum() : type_(Type::AVRO_NULL) { }

    /* Makes a new AVRO_BOOL datum whose value is of type bool.*/
    GenericDatum(bool v) : type_(Type::AVRO_BOOL), value_(v) { }

    /* Makes a new AVRO_INT datum whose value is of type int32_t.*/
    GenericDatum(int32_t v) : type_(Type::AVRO_INT), value_(v) { }

    /* Makes a new AVRO_LONG datum whose value is of type int64_t*/

    GenericDatum(int64_t v) : type_(Type::AVRO_LONG), value_(v) { }

    /* Makes a new AVRO_FLOAT datum whose value is of type float*/
    GenericDatum(float v) : type_(Type::AVRO_FLOAT), value_(v) { }

    /* Makes a new AVRO_DOUBLE datum whose value is of type double.*/
    GenericDatum(double v) : type_(Type::AVRO_DOUBLE), value_(v) { }

    /* Makes a new AVRO_STRING datum whose value is of type std::string.*/
    GenericDatum(const std::string& v) : type_(Type::AVRO_STRING), value_(v) { }

    /* Makes a new AVRO_BYTES datum whose value is of type std::vector<uint8_t>*/
    GenericDatum(const std::vector<uint8_t>& v) :
    type_(Type::AVRO_BYTES), value_(v) { }

    /* Constructs a datum corresponding to the given avro type. The value will the appropriate default corresponding to the data type.
        @param schema The schema that defines the avro type.*/
    GenericDatum(const NodePtr& schema);

    /* Constructs a datum corresponding to the given avro type and set the value. 
        @param schema The schema that defines the avro type.
        @param v The value for this type.*/
    template<typename T>
    GenericDatum(const NodePtr& schema, const T& v) :
    type_(schema->type()) {
      init(schema);
      *boost::any_cast<T>(&value_) = v;
    }

    /* Constructs a datum corresponding to the given avro type. The value will the appropriate default corresponding to the data type.
        @param schema The schema that defines the avro type.*/
    GenericDatum(const ValidSchema& schema);
  };

  /* The base class for all generic type for containers.*/
  class GenericContainer {
    NodePtr schema_;
    static void assertType(const NodePtr& schema, Type type);
  protected:

    /* Constructs a container corresponding to the given schema.*/
    GenericContainer(Type type, const NodePtr& s) : schema_(s) {
      assertType(s, type);
    }

  public:

    /* Returns the schema for this object*/
    const NodePtr& schema() const {
      return schema_;
    }
  };

  /* The generic container for Avro records.*/
  class GenericRecord : public GenericContainer {
    std::vector<GenericDatum> fields_;
  public:

    /* Constructs a generic record corresponding to the given schema schema, which should be of Avro type record.*/
    GenericRecord(const NodePtr& schema);

    /* Returns the number of fields in the current record.*/
    size_t fieldCount() const {
      return fields_.size();
    }

    /* Returns index of the field with the given name*/
    size_t fieldIndex(const std::string& name) const {
      size_t index = 0;
      if (!schema()->nameIndex(name, index)) {
        throw Exception("Invalid field name: " + name);
      }
      return index;
    }

    /* Returns true if a field with the given name is located in this return false otherwise*/
    bool hasField(const std::string& name) const {
      size_t index = 0;
      return schema()->nameIndex(name, index);
    }

    /* Returns the field with the given name.*/
    const GenericDatum& field(const std::string& name) const {
      return fieldAt(fieldIndex(name));
    }

    /**
     * Returns the reference to the field with the given name \p name,
     * which can be used to change the contents.
     */
    GenericDatum& field(const std::string& name) {
      return fieldAt(fieldIndex(name));
    }

    /**
     * Returns the field at the given position \p pos.
     */
    const GenericDatum& fieldAt(size_t pos) const {
      return fields_[pos];
    }

    /**
     * Returns the reference to the field at the given position \p pos,
     * which can be used to change the contents.
     */
    GenericDatum& fieldAt(size_t pos) {
      return fields_[pos];
    }

    /**
     * Replaces the field at the given position \p pos with \p v.
     */
    void setFieldAt(size_t pos, const GenericDatum& v) {
      // assertSameType(v, schema()->leafAt(pos));    
      fields_[pos] = v;
    }
  };

  inline Type GenericDatum::type() const {
    return type_;
  }

  template<typename T> T& GenericDatum::value() {
    return *boost::any_cast<T>(&value_);
  }

  template<typename T> const T& GenericDatum::value() const {
    return *boost::any_cast<T>(&value_);
  }

}
#endif // avro_GenericDatum_hh__
