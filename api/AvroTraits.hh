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

#ifndef avro_AvroTraits_hh__
#define avro_AvroTraits_hh__

#include "Types.hh"

/* This header contains type traits and similar utilities used by the library*/
namespace avro {

  /**
   * Define an is_serializable trait for types we can serialize natively. 
   * New types will need to define the trait as well.
   */
  template <typename T>
  struct is_serializable : public boost::false_type {
  };

  template <typename T>
  struct is_promotable : public boost::false_type {
  };

  template <typename T>
  struct type_to_avro {
    static const Type type = Type::AVRO_NUM_TYPES;
  };

  /* Define primitive types*/
  template <>
  struct is_promotable< int32_t > : public boost::true_type {
  };

  template <>
  struct is_serializable< int32_t > : public boost::true_type {
  };

  template <>
  struct type_to_avro< int32_t > {
    static const Type type = Type::AVRO_INT;
  };

  template <>
  struct is_promotable< int64_t > : public boost::true_type {
  };

  template <>
  struct is_serializable< int64_t > : public boost::true_type {
  };

  template <>
  struct type_to_avro< int64_t > {
    static const Type type = Type::AVRO_LONG;
  };

  template <>
  struct is_promotable< float > : public boost::true_type {
  };

  template <>
  struct is_serializable< float > : public boost::true_type {
  };

  template <>
  struct type_to_avro< float > {
    static const Type type = Type::AVRO_FLOAT;
  };

  template <>
  struct is_serializable< double > : public boost::true_type {
  };

  template <>
  struct type_to_avro< double > {
    static const Type type = Type::AVRO_DOUBLE;
  };

  template <>
  struct is_serializable< bool > : public boost::true_type {
  };

  template <>
  struct type_to_avro< bool > {
    static const Type type = Type::AVRO_BOOL;
  };

  template <>
  struct is_serializable< Null > : public boost::true_type {
  };

  template <>
  struct type_to_avro< Null > {
    static const Type type = Type::AVRO_NULL;
  };

  template <>
  struct is_serializable< std::string > : public boost::true_type {
  };

  template <>
  struct type_to_avro< std::string > {
    static const Type type = Type::AVRO_STRING;
  };

  template <>
  struct is_serializable< std::vector<uint8_t> > : public boost::true_type {
  };

  template <>
  struct type_to_avro< std::vector<uint8_t> > {
    static const Type type = Type::AVRO_BYTES;
  };

}

#endif
