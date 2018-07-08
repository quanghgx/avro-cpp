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

        using std::shared_ptr;
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
                case AVRO_NULL:
                    return make_shared<Production>(1, Symbol::nullSymbol());
                case AVRO_BOOL:
                    return make_shared<Production>(1, Symbol::boolSymbol());
                case AVRO_INT:
                    return make_shared<Production>(1, Symbol::intSymbol());
                case AVRO_LONG:
                    return make_shared<Production>(1, Symbol::longSymbol());
                case AVRO_FLOAT:
                    return make_shared<Production>(1, Symbol::floatSymbol());
                case AVRO_DOUBLE:
                    return make_shared<Production>(1, Symbol::doubleSymbol());
                case AVRO_STRING:
                    return make_shared<Production>(1, Symbol::stringSymbol());
                case AVRO_BYTES:
                    return make_shared<Production>(1, Symbol::bytesSymbol());
                case AVRO_FIXED:
                {
                    ProductionPtr result = make_shared<Production>();
                    result->push_back(Symbol::sizeCheckSymbol(n->fixedSize()));
                    result->push_back(Symbol::fixedSymbol());
                    m[n] = result;
                    return result;
                }
                case AVRO_RECORD:
                {
                    ProductionPtr result = make_shared<Production>();

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
                case AVRO_ENUM:
                {
                    ProductionPtr result = make_shared<Production>();
                    result->push_back(Symbol::sizeCheckSymbol(n->names()));
                    result->push_back(Symbol::enumSymbol());
                    m[n] = result;
                    return result;
                }
                case AVRO_ARRAY:
                {
                    ProductionPtr result = make_shared<Production>();
                    result->push_back(Symbol::arrayEndSymbol());
                    result->push_back(Symbol::repeater(doGenerate(n->leafAt(0), m), true));
                    result->push_back(Symbol::arrayStartSymbol());
                    return result;
                }
                case AVRO_MAP:
                {
                    ProductionPtr pp = doGenerate(n->leafAt(1), m);
                    ProductionPtr v(new Production(*pp));
                    v->push_back(Symbol::stringSymbol());
                    ProductionPtr result = make_shared<Production>();
                    result->push_back(Symbol::mapEndSymbol());
                    result->push_back(Symbol::repeater(v, false));
                    result->push_back(Symbol::mapStartSymbol());
                    return result;
                }
                case AVRO_UNION:
                {
                    vector<ProductionPtr> vv;
                    size_t c = n->leaves();
                    vv.reserve(c);
                    for (size_t i = 0; i < c; ++i) {
                        vv.push_back(doGenerate(n->leafAt(i), m));
                    }
                    ProductionPtr result = make_shared<Production>();
                    result->push_back(Symbol::alternative(vv));
                    result->push_back(Symbol::unionSymbol());
                    return result;
                }
                case AVRO_SYMBOLIC:
                {
                    shared_ptr<NodeSymbolic> ns = static_pointer_cast<NodeSymbolic>(n);
                    NodePtr nn = ns->getNode();
                    map<NodePtr, ProductionPtr>::iterator it =
                      m.find(nn);
                    if (it != m.end() && it->second) {
                        return it->second;
                    } else {
                        m[nn] = ProductionPtr();
                        return make_shared<Production>(1, Symbol::placeholder(nn));
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
            const shared_ptr<Decoder> base;
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
            void decodeFixed(size_t n, vector<uint8_t>& value);
            void skipFixed(size_t n);
            size_t decodeEnum();
            size_t arrayStart();
            size_t arrayNext();
            size_t skipArray();
            size_t mapStart();
            size_t mapNext();
            size_t skipMap();
            size_t decodeUnionIndex();

        public:

            ValidatingDecoder(const ValidSchema& s, const shared_ptr<Decoder> b) :
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
        void ValidatingDecoder<P>::decodeFixed(size_t n, vector<uint8_t>& value) {
            parser.advance(Symbol::sFixed);
            parser.assertSize(n);
            base->decodeFixed(n, value);
        }

        template <typename P>
        void ValidatingDecoder<P>::skipFixed(size_t n) {
            parser.advance(Symbol::sFixed);
            parser.assertSize(n);
            base->skipFixed(n);
        }

        template <typename P>
        size_t ValidatingDecoder<P>::decodeEnum() {
            parser.advance(Symbol::sEnum);
            size_t result = base->decodeEnum();
            parser.assertLessThanSize(result);
            return result;
        }

        template <typename P>
        size_t ValidatingDecoder<P>::arrayStart() {
            parser.advance(Symbol::sArrayStart);
            size_t result = base->arrayStart();
            if (result == 0) {
                parser.popRepeater();
                parser.advance(Symbol::sArrayEnd);
            } else {
                parser.setRepeatCount(result);
            }
            return result;
        }

        template <typename P>
        size_t ValidatingDecoder<P>::arrayNext() {
            size_t result = base->arrayNext();
            if (result == 0) {
                parser.popRepeater();
                parser.advance(Symbol::sArrayEnd);
            } else {
                parser.setRepeatCount(result);
            }
            return result;
        }

        template <typename P>
        size_t ValidatingDecoder<P>::skipArray() {
            parser.advance(Symbol::sArrayStart);
            size_t n = base->skipArray();
            if (n == 0) {
                parser.pop();
            } else {
                parser.setRepeatCount(n);
                parser.skip(*base);
            }
            parser.advance(Symbol::sArrayEnd);
            return 0;
        }

        template <typename P>
        size_t ValidatingDecoder<P>::mapStart() {
            parser.advance(Symbol::sMapStart);
            size_t result = base->mapStart();
            if (result == 0) {
                parser.popRepeater();
                parser.advance(Symbol::sMapEnd);
            } else {
                parser.setRepeatCount(result);
            }
            return result;
        }

        template <typename P>
        size_t ValidatingDecoder<P>::mapNext() {
            size_t result = base->mapNext();
            if (result == 0) {
                parser.popRepeater();
                parser.advance(Symbol::sMapEnd);
            } else {
                parser.setRepeatCount(result);
            }
            return result;
        }

        template <typename P>
        size_t ValidatingDecoder<P>::skipMap() {
            parser.advance(Symbol::sMapStart);
            size_t n = base->skipMap();
            if (n == 0) {
                parser.pop();
            } else {
                parser.setRepeatCount(n);
                parser.skip(*base);
            }
            parser.advance(Symbol::sMapEnd);
            return 0;
        }

        template <typename P>
        size_t ValidatingDecoder<P>::decodeUnionIndex() {
            parser.advance(Symbol::sUnion);
            size_t result = base->decodeUnionIndex();
            parser.selectBranch(result);
            return result;
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
            void encodeFixed(const uint8_t *bytes, size_t len);
            void encodeEnum(size_t e);
            void arrayStart();
            void arrayEnd();
            void mapStart();
            void mapEnd();
            void setItemCount(size_t count);
            void startItem();
            void encodeUnionIndex(size_t e);
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
        void ValidatingEncoder<P>::encodeFixed(const uint8_t *bytes, size_t len) {
            parser_.advance(Symbol::sFixed);
            parser_.assertSize(len);
            base_->encodeFixed(bytes, len);
        }

        template<typename P>
        void ValidatingEncoder<P>::encodeEnum(size_t e) {
            parser_.advance(Symbol::sEnum);
            parser_.assertLessThanSize(e);
            base_->encodeEnum(e);
        }

        template<typename P>
        void ValidatingEncoder<P>::arrayStart() {
            parser_.advance(Symbol::sArrayStart);
            base_->arrayStart();
        }

        template<typename P>
        void ValidatingEncoder<P>::arrayEnd() {
            parser_.popRepeater();
            parser_.advance(Symbol::sArrayEnd);
            base_->arrayEnd();
        }

        template<typename P>
        void ValidatingEncoder<P>::mapStart() {
            parser_.advance(Symbol::sMapStart);
            base_->mapStart();
        }

        template<typename P>
        void ValidatingEncoder<P>::mapEnd() {
            parser_.popRepeater();
            parser_.advance(Symbol::sMapEnd);
            base_->mapEnd();
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

        template<typename P>
        void ValidatingEncoder<P>::encodeUnionIndex(size_t e) {
            parser_.advance(Symbol::sUnion);
            parser_.selectBranch(e);
            base_->encodeUnionIndex(e);
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

