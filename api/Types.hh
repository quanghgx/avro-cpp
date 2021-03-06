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

#ifndef avro_Types_hh__
#define avro_Types_hh__

#include <iostream>


namespace avro {

  /* The "type" for the schema.*/
  enum class Type : int {
    AVRO_STRING, /* String */
    AVRO_BYTES, /* Sequence of variable length bytes data */
    AVRO_INT, /* 32-bit integer */
    AVRO_LONG, /* 64-bit integer */
    AVRO_FLOAT, /* Floating point number */
    AVRO_DOUBLE, /* Double precision floating point number */
    AVRO_BOOL, /* Boolean value */
    AVRO_NULL, /* Null */

    AVRO_RECORD, /* Record, a sequence of fields */

    AVRO_NUM_TYPES, /* Marker */

    /* The following is a pseudo-type used in implementation*/
    AVRO_SYMBOLIC = Type::AVRO_NUM_TYPES, /* User internally to avoid circular references. */
    AVRO_UNKNOWN = -1 /* Used internally. */

  };

  /* Get type as integer value*/
  inline int type_as_integer(Type t) {
    return static_cast<std::underlying_type<Type>::type> (t);
  }

  /* Returns true if and only if the given type is a primitive. Primitive types are: string, bytes, int, long, float, double, boolean and 
     null*/
  inline bool isPrimitive(Type t) {
    return (t >= Type::AVRO_STRING) && (t < Type::AVRO_RECORD);
  }

  /* Returns true if and only if the given type is a non primitive valid type. Primitive types are: string, bytes, int, long, float, double, 
     boolean and null*/
  inline bool isCompound(Type t) {
    return (t >= Type::AVRO_RECORD) && (t < Type::AVRO_NUM_TYPES);
  }

  /* Returns true if and only if the given type is a valid avro type.*/
  inline bool isAvroType(Type t) {
    return (t >= Type::AVRO_STRING) && (t < Type::AVRO_NUM_TYPES);
  }

  /* Returns true if and only if the given type is within the valid range of enumeration.*/
  inline bool isAvroTypeOrPseudoType(Type t) {
    return (t >= Type::AVRO_STRING) && (t <= Type::AVRO_NUM_TYPES);
  }

  /* Converts the given type into a string. Useful for generating messages.*/
  const std::string toString(Type type);

  /* Writes a string form of the given type into the given ostream.*/
  std::ostream &operator<<(std::ostream &os, avro::Type type);

  /* define a type to identify Null in template functions*/
  struct Null {
  };

  /* Writes schema for null type to OS
     @param os The ostream to write to
     @param null The value to be written*/
  std::ostream& operator<<(std::ostream &os, const Null &null);
}

#endif
