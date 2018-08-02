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

#include "ValidatingCodec.hh"

#include <string>
#include <map>
#include <algorithm>
#include <memory>
#include <boost/any.hpp>

#include "ValidSchema.hh"
#include "Decoder.hh"
#include "Encoder.hh"
#include "NodeImpl.hh"

namespace avro {

  namespace parsing {

    using std::weak_ptr;
    using std::static_pointer_cast;
    using std::make_shared;

    using std::map;
    using std::vector;
    using std::pair;
    using std::string;
    using std::reverse;
    using std::ostringstream;

    /** Follows the design of Avro Parser in Java. */
    ProductionPtr ValidatingGrammarGenerator::generate(const NodePtr& n) {
      map<NodePtr, ProductionPtr> m;
      ProductionPtr result = doGenerate(n, m);
      fixup(result, m);
      return result;
    }

    Symbol ValidatingGrammarGenerator::generate(const ValidSchema& schema) {
      ProductionPtr r = generate(schema.root());
      return Symbol::rootSymbol(r);
    }

    ProductionPtr ValidatingGrammarGenerator::doGenerate(const NodePtr& n,
      map<NodePtr, ProductionPtr> &m) {
      switch (n->type()) {
        case Type::AVRO_NULL:
          return std::make_shared<Production>(1, Symbol::nullSymbol());
        case Type::AVRO_BOOL:
          return std::make_shared<Production>(1, Symbol::boolSymbol());
        case Type::AVRO_INT:
          return std::make_shared<Production>(1, Symbol::intSymbol());
        case Type::AVRO_LONG:
          return std::make_shared<Production>(1, Symbol::longSymbol());
        case Type::AVRO_FLOAT:
          return std::make_shared<Production>(1, Symbol::floatSymbol());
        case Type::AVRO_DOUBLE:
          return std::make_shared<Production>(1, Symbol::doubleSymbol());
        case Type::AVRO_STRING:
          return std::make_shared<Production>(1, Symbol::stringSymbol());
        case Type::AVRO_BYTES:
          return std::make_shared<Production>(1, Symbol::bytesSymbol());
        case Type::AVRO_RECORD:
        {
          ProductionPtr result = std::make_shared<Production>();

          m.erase(n);
          size_t c = n->leaves();
          for (size_t i = 0; i < c; ++i) {
            const NodePtr& leaf = n->leafAt(i);
            ProductionPtr v = doGenerate(leaf, m);
            copy(v->rbegin(), v->rend(), back_inserter(*result));
          }
          reverse(result->begin(), result->end());

          m[n] = result;
          return result;
        }
        case Type::AVRO_SYMBOLIC:
        {
          std::shared_ptr<NodeSymbolic> ns = static_pointer_cast<NodeSymbolic>(n);
          NodePtr nn = ns->getNode();
          map<NodePtr, ProductionPtr>::iterator it =
            m.find(nn);
          if (it != m.end() && it->second) {
            return it->second;
          } else {
            m[nn] = ProductionPtr();
            return std::make_shared<Production>(1, Symbol::placeholder(nn));
          }
        }
        default:
          throw Exception("Unknown node type");
      }
    }

    struct DummyHandler {

      size_t handle(const Symbol& s) {
        return 0;
      }
    };

    template <typename P>
    class ValidatingDecoder : public Decoder {
      const std::shared_ptr<Decoder> base;
      DummyHandler handler_;
      P parser;

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

    public:

      ValidatingDecoder(const ValidSchema& s, const std::shared_ptr<Decoder> b) :
      base(b),
      parser(ValidatingGrammarGenerator().generate(s), NULL, handler_) {
      }

    };

    template <typename P>
    void ValidatingDecoder<P>::init(InputStream& is) {
      base->init(is);
    }

    template <typename P>
    void ValidatingDecoder<P>::decodeNull() {
      parser.advance(Symbol::sNull);
      base->decodeNull();
    }

    template <typename P>
    bool ValidatingDecoder<P>::decodeBool() {
      parser.advance(Symbol::sBool);
      return base->decodeBool();
    }

    template <typename P>
    int32_t ValidatingDecoder<P>::decodeInt() {
      parser.advance(Symbol::sInt);
      return base->decodeInt();
    }

    template <typename P>
    int64_t ValidatingDecoder<P>::decodeLong() {
      parser.advance(Symbol::sLong);
      return base->decodeLong();
    }

    template <typename P>
    float ValidatingDecoder<P>::decodeFloat() {
      parser.advance(Symbol::sFloat);
      return base->decodeFloat();
    }

    template <typename P>
    double ValidatingDecoder<P>::decodeDouble() {
      parser.advance(Symbol::sDouble);
      return base->decodeDouble();
    }

    template <typename P>
    void ValidatingDecoder<P>::decodeString(string& value) {
      parser.advance(Symbol::sString);
      base->decodeString(value);
    }

    template <typename P>
    void ValidatingDecoder<P>::skipString() {
      parser.advance(Symbol::sString);
      base->skipString();
    }

    template <typename P>
    void ValidatingDecoder<P>::decodeBytes(vector<uint8_t>& value) {
      parser.advance(Symbol::sBytes);
      base->decodeBytes(value);
    }

    template <typename P>
    void ValidatingDecoder<P>::skipBytes() {
      parser.advance(Symbol::sBytes);
      base->skipBytes();
    }

    template <typename P>
    class ValidatingEncoder : public Encoder {
      DummyHandler handler_;
      P parser_;
      EncoderPtr base_;

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

      ValidatingEncoder(const ValidSchema& schema, const EncoderPtr& base) :
      parser_(ValidatingGrammarGenerator().generate(schema), NULL, handler_),
      base_(base) {
      }
    };

    template<typename P>
    void ValidatingEncoder<P>::init(OutputStream& os) {
      base_->init(os);
    }

    template<typename P>
    void ValidatingEncoder<P>::flush() {
      base_->flush();
    }

    template<typename P>
    void ValidatingEncoder<P>::encodeNull() {
      parser_.advance(Symbol::sNull);
      base_->encodeNull();
    }

    template<typename P>
    void ValidatingEncoder<P>::encodeBool(bool b) {
      parser_.advance(Symbol::sBool);
      base_->encodeBool(b);
    }

    template<typename P>
    void ValidatingEncoder<P>::encodeInt(int32_t i) {
      parser_.advance(Symbol::sInt);
      base_->encodeInt(i);
    }

    template<typename P>
    void ValidatingEncoder<P>::encodeLong(int64_t l) {
      parser_.advance(Symbol::sLong);
      base_->encodeLong(l);
    }

    template<typename P>
    void ValidatingEncoder<P>::encodeFloat(float f) {
      parser_.advance(Symbol::sFloat);
      base_->encodeFloat(f);
    }

    template<typename P>
    void ValidatingEncoder<P>::encodeDouble(double d) {
      parser_.advance(Symbol::sDouble);
      base_->encodeDouble(d);
    }

    template<typename P>
    void ValidatingEncoder<P>::encodeString(const std::string& s) {
      parser_.advance(Symbol::sString);
      base_->encodeString(s);
    }

    template<typename P>
    void ValidatingEncoder<P>::encodeBytes(const uint8_t *bytes, size_t len) {
      parser_.advance(Symbol::sBytes);
      base_->encodeBytes(bytes, len);
    }  

    template<typename P>
    void ValidatingEncoder<P>::setItemCount(size_t count) {
      parser_.setRepeatCount(count);
      base_->setItemCount(count);
    }

    template<typename P>
    void ValidatingEncoder<P>::startItem() {
      if (parser_.top() != Symbol::sRepeater) {
        throw Exception("startItem at not an item boundary");
      }
      base_->startItem();
    }

  } // namespace parsing

  DecoderPtr validatingDecoder(const ValidSchema& s,
    const DecoderPtr& base) {
    return std::make_shared<parsing::ValidatingDecoder<
      parsing::SimpleParser<parsing::DummyHandler> > >(s, base);
  }

  EncoderPtr validatingEncoder(const ValidSchema& schema, const EncoderPtr& base) {
    return std::make_shared<parsing::ValidatingEncoder<
      parsing::SimpleParser<parsing::DummyHandler> > >(schema, base);
  }

}

