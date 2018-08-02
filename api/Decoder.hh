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

#ifndef avro_Decoder_hh__
#define avro_Decoder_hh__

#include <cstdint>
#include <string>
#include <vector>
#include <memory>

#include "ValidSchema.hh"
#include "Stream.hh"


/* Low level support for decoding avro values. This class has two types of functions.  One type of functions support decoding of leaf 
   values (for example, decodeLong and decodeString). These functions have analogs in Encoder.
 
   The other type of functions support decoding of maps and arrays. These functions are arrayStart, startItem, and arrayEnd (and similar 
   functions for maps)*/
namespace avro {

  /* Decoder is an interface implemented by every decoder capable of decoding Avro data*/
  class Decoder {
  public:

    virtual ~Decoder() { };

    /* All future decoding will come from is, which should be valid until replaced by another call to init() or this Decoder is destructed*/
    virtual void init(InputStream& is) = 0;

    /* Decodes a null from the current stream*/
    virtual void decodeNull() = 0;

    /* Decodes a bool from the current stream*/
    virtual bool decodeBool() = 0;

    /* Decodes a 32-bit int from the current stream*/
    virtual int32_t decodeInt() = 0;

    /* Decodes a 64-bit signed int from the current stream*/
    virtual int64_t decodeLong() = 0;

    /* Decodes a single-precision floating point number from current stream*/
    virtual float decodeFloat() = 0;

    /* Decodes a double-precision floating point number from current stream*/
    virtual double decodeDouble() = 0;

    /* Decodes a UTF-8 string from the current stream*/
    std::string decodeString() {
      std::string result;
      decodeString(result);
      return result;
    }

    /* Decodes a UTF-8 string from the stream and assigns it to value*/
    virtual void decodeString(std::string& value) = 0;

    /* Skips a string on the current stream*/
    virtual void skipString() = 0;

    /* Decodes arbitrary binary data from the current stream*/
    std::vector<uint8_t> decodeBytes() {
      std::vector<uint8_t> result;
      decodeBytes(result);
      return result;
    }

    /* Decodes arbitrary binary data from the current stream and puts it in value*/
    virtual void decodeBytes(std::vector<uint8_t>& value) = 0;

    /* Skips bytes on the current stream*/
    virtual void skipBytes() = 0;   
  };

  /* Shared pointer to Decoder*/
  typedef std::shared_ptr<Decoder> DecoderPtr;

  /* ResolvingDecoder is derived from \ref Decoder, with an additional function to obtain the field ordering of fields within a record*/
  class ResolvingDecoder : public Decoder {
  public:
    /* Returns the order of fields for records. The order of fields could be different from the order of their order in the schema because 
       the writer's field order could be different. In order to avoid buffering and later use, we return the values in the writer's field 
       order*/
    virtual const std::vector<size_t>& fieldOrder() = 0;
  };

  /* Shared pointer to ResolvingDecoder*/
  typedef std::shared_ptr<ResolvingDecoder> ResolvingDecoderPtr;

  /* Returns an decoder that can decode binary Avro standard*/
  DecoderPtr binaryDecoder();

  /* Returns an decoder that validates sequence of calls to an underlying Decoder against the given schema*/
  DecoderPtr validatingDecoder(const ValidSchema& schema,
    const DecoderPtr& base);

  /* Returns an decoder that can decode Avro standard for JSON*/
  DecoderPtr jsonDecoder(const ValidSchema& schema);

  /* Returns a decoder that decodes avro data from base written according to writerSchema and resolves against readerSchema. The client 
     uses the decoder as if the data were written using readerSchema.
     @FIXME: Handle out of order fields*/
  ResolvingDecoderPtr resolvingDecoder(const ValidSchema& writer, const ValidSchema& reader, const DecoderPtr& base);
}

#endif
