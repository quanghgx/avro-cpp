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

#include <string>
#include <map>
#include <algorithm>
#include <ctype.h>
#include <memory>
#include <boost/any.hpp>
#include <boost/math/special_functions/fpclassify.hpp>

#include "ValidatingCodec.hh"
#include "Symbol.hh"
#include "ValidSchema.hh"
#include "Decoder.hh"
#include "Encoder.hh"
#include "NodeImpl.hh"

#include "../json/JsonIO.hh"

namespace avro {

  namespace parsing {

    using std::static_pointer_cast;

    using std::map;
    using std::vector;
    using std::string;
    using std::reverse;
    using std::ostringstream;
    using std::istringstream;

    using avro::json::JsonParser;
    using avro::json::JsonGenerator;
    using avro::json::JsonNullFormatter;

    class JsonGrammarGenerator : public ValidatingGrammarGenerator {
      ProductionPtr doGenerate(const NodePtr& n,
        std::map<NodePtr, ProductionPtr> &m);
    };

    static std::string nameOf(const NodePtr& n) {
      if (n->hasName()) {
        return n->name();
      }
      std::ostringstream oss;
      oss << n->type();
      return oss.str();
    }

    ProductionPtr JsonGrammarGenerator::doGenerate(const NodePtr& n,
      std::map<NodePtr, ProductionPtr> &m) {
      switch (n->type()) {
        case Type::AVRO_NULL:
        case Type::AVRO_BOOL:
        case Type::AVRO_INT:
        case Type::AVRO_LONG:
        case Type::AVRO_FLOAT:
        case Type::AVRO_DOUBLE:
        case Type::AVRO_STRING:
        case Type::AVRO_BYTES:
        case Type::AVRO_SYMBOLIC:
          return ValidatingGrammarGenerator::doGenerate(n, m);
        case Type::AVRO_RECORD:
        {
          ProductionPtr result = std::make_shared<Production>();

          m.erase(n);

          size_t c = n->leaves();
          result->reserve(2 + 2 * c);
          result->push_back(Symbol::recordStartSymbol());
          for (size_t i = 0; i < c; ++i) {
            const NodePtr& leaf = n->leafAt(i);
            ProductionPtr v = doGenerate(leaf, m);
            result->push_back(Symbol::fieldSymbol(n->nameAt(i)));
            copy(v->rbegin(), v->rend(), back_inserter(*result));
          }
          result->push_back(Symbol::recordEndSymbol());
          reverse(result->begin(), result->end());

          m[n] = result;
          return result;
        }
        default:
          throw Exception("Unknown node type");
      }
    }

    static void expectToken(JsonParser& in, JsonParser::Token tk) {
      in.expectToken(tk);
    }

    class JsonDecoderHandler {
      JsonParser& in_;
    public:

      JsonDecoderHandler(JsonParser& p) : in_(p) {
      }

      size_t handle(const Symbol& s) {
        switch (s.kind()) {
          case Symbol::sRecordStart:
            expectToken(in_, JsonParser::tkObjectStart);
            break;
          case Symbol::sRecordEnd:
            expectToken(in_, JsonParser::tkObjectEnd);
            break;
          case Symbol::sField:
            expectToken(in_, JsonParser::tkString);
            if (s.extra<string>() != in_.stringValue()) {
              throw Exception("Incorrect field");
            }
            break;
          default:
            break;
        }
        return 0;
      }
    };

    template <typename P>
    class JsonDecoder : public Decoder {
      JsonParser in_;
      JsonDecoderHandler handler_;
      P parser_;

      void init(InputStream& is);
      void decodeNull();
      bool decodeBool();
      int32_t decodeInt();
      int64_t decodeLong();
      float decodeFloat();
      double decodeDouble();
      void decodeString(string& value);
      void skipString();
      void decodeBytes(vector<uint8_t>& value);
      void skipBytes();

      void expect(JsonParser::Token tk);
      void skipComposite();
    public:

      JsonDecoder(const ValidSchema& s) :
      handler_(in_),
      parser_(JsonGrammarGenerator().generate(s), NULL, handler_) {
      }

    };

    template <typename P>
    void JsonDecoder<P>::init(InputStream& is) {
      in_.init(is);
    }

    template <typename P>
    void JsonDecoder<P>::expect(JsonParser::Token tk) {
      expectToken(in_, tk);
    }

    template <typename P>
    void JsonDecoder<P>::decodeNull() {
      parser_.advance(Symbol::sNull);
      expect(JsonParser::tkNull);
    }

    template <typename P>
    bool JsonDecoder<P>::decodeBool() {
      parser_.advance(Symbol::sBool);
      expect(JsonParser::tkBool);
      bool result = in_.boolValue();
      return result;
    }

    template <typename P>
    int32_t JsonDecoder<P>::decodeInt() {
      parser_.advance(Symbol::sInt);
      expect(JsonParser::tkLong);
      int64_t result = in_.longValue();
      if (result < INT32_MIN || result > INT32_MAX) {
        throw Exception(boost::format("Value out of range for Avro int: %1%")
          % result);
      }
      return static_cast<int32_t> (result);
    }

    template <typename P>
    int64_t JsonDecoder<P>::decodeLong() {
      parser_.advance(Symbol::sLong);
      expect(JsonParser::tkLong);
      int64_t result = in_.longValue();
      return result;
    }

    template <typename P>
    float JsonDecoder<P>::decodeFloat() {
      parser_.advance(Symbol::sFloat);
      expect(JsonParser::tkDouble);
      double result = in_.doubleValue();
      return static_cast<float> (result);
    }

    template <typename P>
    double JsonDecoder<P>::decodeDouble() {
      parser_.advance(Symbol::sDouble);
      expect(JsonParser::tkDouble);
      double result = in_.doubleValue();
      return result;
    }

    template <typename P>
    void JsonDecoder<P>::decodeString(string& value) {
      parser_.advance(Symbol::sString);
      expect(JsonParser::tkString);
      value = in_.stringValue();
    }

    template <typename P>
    void JsonDecoder<P>::skipString() {
      parser_.advance(Symbol::sString);
      expect(JsonParser::tkString);
    }

    static vector<uint8_t> toBytes(const string& s) {
      return vector<uint8_t>(s.begin(), s.end());
    }

    template <typename P>
    void JsonDecoder<P>::decodeBytes(vector<uint8_t>& value) {
      parser_.advance(Symbol::sBytes);
      expect(JsonParser::tkString);
      value = toBytes(in_.stringValue());
    }

    template <typename P>
    void JsonDecoder<P>::skipBytes() {
      parser_.advance(Symbol::sBytes);
      expect(JsonParser::tkString);
    }   

    template<typename P>
    void JsonDecoder<P>::skipComposite() {
      size_t level = 0;
      for (;;) {
        switch (in_.advance()) {
          case JsonParser::tkArrayStart:
          case JsonParser::tkObjectStart:
            ++level;
            continue;
          case JsonParser::tkArrayEnd:
          case JsonParser::tkObjectEnd:
            if (level == 0) {
              return;
            }
            --level;
            continue;
          default:
            continue;
        }
      }
    }
   
    template<typename F = JsonNullFormatter>
    class JsonHandler {
      JsonGenerator<F>& generator_;
    public:

      JsonHandler(JsonGenerator<F>& g) : generator_(g) {
      }

      size_t handle(const Symbol& s) {
        switch (s.kind()) {
          case Symbol::sRecordStart:
            generator_.objectStart();
            break;
          case Symbol::sRecordEnd:
            generator_.objectEnd();
            break;
          case Symbol::sField:
            generator_.encodeString(s.extra<string>());
            break;
          default:
            break;
        }
        return 0;
      }
    };

    template <typename P, typename F = JsonNullFormatter>
    class JsonEncoder : public Encoder {
      JsonGenerator<F> out_;
      JsonHandler<F> handler_;
      P parser_;

      void init(OutputStream& os);
      void flush();
      void encodeNull();
      void encodeBool(bool b);
      void encodeInt(int32_t i);
      void encodeLong(int64_t l);
      void encodeFloat(float f);
      void encodeDouble(double d);
      void encodeString(const std::string& s);
      void encodeBytes(const uint8_t *bytes, size_t len);      
      void setItemCount(size_t count);
      void startItem();
    public:

      JsonEncoder(const ValidSchema& schema) :
      handler_(out_),
      parser_(JsonGrammarGenerator().generate(schema), NULL, handler_) {
      }
    };

    template<typename P, typename F>
    void JsonEncoder<P, F>::init(OutputStream& os) {
      out_.init(os);
    }

    template<typename P, typename F>
    void JsonEncoder<P, F>::flush() {
      parser_.processImplicitActions();
      out_.flush();
    }

    template<typename P, typename F>
    void JsonEncoder<P, F>::encodeNull() {
      parser_.advance(Symbol::sNull);
      out_.encodeNull();
    }

    template<typename P, typename F>
    void JsonEncoder<P, F>::encodeBool(bool b) {
      parser_.advance(Symbol::sBool);
      out_.encodeBool(b);
    }

    template<typename P, typename F>
    void JsonEncoder<P, F>::encodeInt(int32_t i) {
      parser_.advance(Symbol::sInt);
      out_.encodeNumber(i);
    }

    template<typename P, typename F>
    void JsonEncoder<P, F>::encodeLong(int64_t l) {
      parser_.advance(Symbol::sLong);
      out_.encodeNumber(l);
    }

    template<typename P, typename F>
    void JsonEncoder<P, F>::encodeFloat(float f) {
      parser_.advance(Symbol::sFloat);
      if (f == std::numeric_limits<float>::infinity()) {
        out_.encodeString("Infinity");
      } else if (f == -std::numeric_limits<float>::infinity()) {
        out_.encodeString("-Infinity");
      } else if (boost::math::isnan(f)) {
        out_.encodeString("NaN");
      } else {
        out_.encodeNumber(f);
      }
    }

    template<typename P, typename F>
    void JsonEncoder<P, F>::encodeDouble(double d) {
      parser_.advance(Symbol::sDouble);
      if (d == std::numeric_limits<double>::infinity()) {
        out_.encodeString("Infinity");
      } else if (d == -std::numeric_limits<double>::infinity()) {
        out_.encodeString("-Infinity");
      } else if (boost::math::isnan(d)) {
        out_.encodeString("NaN");
      } else {
        out_.encodeNumber(d);
      }
    }

    template<typename P, typename F>
    void JsonEncoder<P, F>::encodeString(const std::string& s) {
      parser_.advance(Symbol::sString);
      out_.encodeString(s);
    }

    template<typename P, typename F>
    void JsonEncoder<P, F>::encodeBytes(const uint8_t *bytes, size_t len) {
      parser_.advance(Symbol::sBytes);
      out_.encodeBinary(bytes, len);
    }    

    template<typename P, typename F>
    void JsonEncoder<P, F>::setItemCount(size_t count) {
      parser_.setRepeatCount(count);
    }

    template<typename P, typename F>
    void JsonEncoder<P, F>::startItem() {
      parser_.processImplicitActions();
      if (parser_.top() != Symbol::sRepeater) {
        throw Exception("startItem at not an item boundary");
      }
    }    

  } // namespace parsing

  DecoderPtr jsonDecoder(const ValidSchema& s) {
    return std::make_shared<parsing::JsonDecoder<
      parsing::SimpleParser<parsing::JsonDecoderHandler> > >(s);
  }

  EncoderPtr jsonEncoder(const ValidSchema& schema) {
    return std::make_shared<parsing::JsonEncoder<
      parsing::SimpleParser<parsing::JsonHandler<avro::json::JsonNullFormatter> >, avro::json::JsonNullFormatter> >(schema);
  }

  EncoderPtr jsonPrettyEncoder(const ValidSchema& schema) {
    return std::make_shared<parsing::JsonEncoder<
      parsing::SimpleParser<parsing::JsonHandler<avro::json::JsonPrettyFormatter> >, avro::json::JsonPrettyFormatter> >(schema);
  }

}

