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

#ifndef avro_Codec_hh__
#define avro_Codec_hh__

#include <string>
#include <vector>
#include <map>
#include <algorithm>

#include "boost/array.hpp"
#include "boost/blank.hpp"

#include "AvroTraits.hh"
#include "Encoder.hh"
#include "Decoder.hh"

/* A bunch of templates and specializations for encoding and decoding specific types.
 
   Primitive AVRO types BOOLEAN, INT, LONG, FLOAT, DOUBLE, STRING and BYTES get decoded to and encoded from C++ types bool, int32_t, 
   int64_t, float, double, std::string and std::vector<uint8_t> respectively. In addition, std::vector<T> for arbitrary type T gets encoded 
   as an Avro array of T. Similarly, std::map<std::string, T> for arbitrary type T gets encoded as an Avro map with value type T.
  
   Users can have their custom types encoded/decoded by specializing avro::codec_traits class for their types.*/
namespace avro {

  typedef boost::blank null;

  template <typename T>
  void encode(Encoder& e, const T& t);

  template <typename T>
  void decode(Decoder& d, T& t);

  /* Codec_traits tells avro how to encode and decode an object of given type. The class is expected to have two static methods:
      static void encode(Encoder& e, const T& value);
      static void decode(Decoder& e, T& value);
     The default is empty.*/
  template <typename T>
  struct codec_traits;

  /* codec_traits for Avro boolean.*/
  template <>
  struct codec_traits<bool> {

    /* Encodes a given value.*/
    static void encode(Encoder& e, bool b) {
      e.encodeBool(b);
    }

    /* Decodes into a given value.*/
    static void decode(Decoder& d, bool& b) {
      b = d.decodeBool();
    }
  };

  /* codec_traits for Avro int.*/
  template <>
  struct codec_traits<int32_t> {

    /* Encodes a given value.*/
    static void encode(Encoder& e, int32_t i) {
      e.encodeInt(i);
    }

    /* Decodes into a given value.*/
    static void decode(Decoder& d, int32_t& i) {
      i = d.decodeInt();
    }
  };

  /* codec_traits for Avro long.*/
  template <>
  struct codec_traits<int64_t> {

    /* Encodes a given value.*/
    static void encode(Encoder& e, int64_t l) {
      e.encodeLong(l);
    }

    /* Decodes into a given value.*/
    static void decode(Decoder& d, int64_t& l) {
      l = d.decodeLong();
    }
  };

  /* codec_traits for Avro float.*/
  template <>
  struct codec_traits<float> {

    /* Encodes a given value.*/
    static void encode(Encoder& e, float f) {
      e.encodeFloat(f);
    }

    /* Decodes into a given value.*/
    static void decode(Decoder& d, float& f) {
      f = d.decodeFloat();
    }
  };

  /* codec_traits for Avro double.*/
  template <>
  struct codec_traits<double> {

    /* Encodes a given value.*/
    static void encode(Encoder& e, double d) {
      e.encodeDouble(d);
    }

    /* Decodes into a given value.*/
    static void decode(Decoder& d, double& dbl) {
      dbl = d.decodeDouble();
    }
  };

  /* codec_traits for Avro string.*/
  template <>
  struct codec_traits<std::string> {

    /* Encodes a given value.*/
    static void encode(Encoder& e, const std::string& s) {
      e.encodeString(s);
    }

    /* Decodes into a given value.*/
    static void decode(Decoder& d, std::string& s) {
      s = d.decodeString();
    }
  };

  /* codec_traits for Avro bytes.*/
  template <>
  struct codec_traits<std::vector<uint8_t> > {

    /* Encodes a given value.*/
    static void encode(Encoder& e, const std::vector<uint8_t>& b) {
      e.encodeBytes(b);
    }

    /* Decodes into a given value.*/
    static void decode(Decoder& d, std::vector<uint8_t>& s) {
      d.decodeBytes(s);
    }
  };


  /* codec_traits for Avro null.*/
  template <>
  struct codec_traits<avro::null> {

    /* Encodes a given value.*/
    static void encode(Encoder& e, const avro::null&) {
      e.encodeNull();
    }

    /* Decodes into a given value.*/
    static void decode(Decoder& d, avro::null&) {
      d.decodeNull();
    }
  };

  /* Generic encoder function that makes use of the codec_traits.*/
  template <typename T>
  void encode(Encoder& e, const T& t) {
    codec_traits<T>::encode(e, t);
  }

  /* Generic decoder function that makes use of the codec_traits.*/
  template <typename T>
  void decode(Decoder& d, T& t) {
    codec_traits<T>::decode(d, t);
  }

}

#endif // avro_Codec_hh__



