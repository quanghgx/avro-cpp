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
#include <stack>
#include <map>
#include <algorithm>
#include <ctype.h>
#include <memory>
#include <boost/any.hpp>
#include <boost/utility.hpp>

#include "ValidatingCodec.hh"
#include "Symbol.hh"
#include "Types.hh"
#include "ValidSchema.hh"
#include "Decoder.hh"
#include "Encoder.hh"
#include "NodeImpl.hh"
#include "Generic.hh"
#include "Stream.hh"

namespace avro {

  namespace parsing {
    
    using std::static_pointer_cast;
    using std::map;
    using std::pair;
    using std::vector;
    using std::string;
    using std::reverse;
    using std::ostringstream;
    using std::istringstream;
    using std::stack;
    using std::find_if;
    using std::make_pair;

    typedef pair<NodePtr, NodePtr> NodePair;

    class ResolvingGrammarGenerator : public ValidatingGrammarGenerator {
      ProductionPtr doGenerate2(const NodePtr& writer,
        const NodePtr& reader, map<NodePair, ProductionPtr> &m,
        map<NodePtr, ProductionPtr> &m2);
      ProductionPtr resolveRecords(const NodePtr& writer,
        const NodePtr& reader, map<NodePair, ProductionPtr> &m,
        map<NodePtr, ProductionPtr> &m2);
      ProductionPtr resolveUnion(const NodePtr& writer,
        const NodePtr& reader, map<NodePair, ProductionPtr> &m,
        map<NodePtr, ProductionPtr> &m2);

      static vector<pair<string, size_t> > fields(const NodePtr& n) {
        vector<pair<string, size_t> > result;
        size_t c = n->names();
        for (size_t i = 0; i < c; ++i) {
          result.push_back(make_pair(n->nameAt(i), i));
        }
        return result;
      }

      static int bestBranch(const NodePtr& writer, const NodePtr& reader);

      ProductionPtr getWriterProduction(const NodePtr& n,
        map<NodePtr, ProductionPtr>& m2);

    public:
      Symbol generate(
        const ValidSchema& writer, const ValidSchema& reader);
    };

    Symbol ResolvingGrammarGenerator::generate(
      const ValidSchema& writer, const ValidSchema& reader) {
      map<NodePtr, ProductionPtr> m2;

      const NodePtr& rr = reader.root();
      const NodePtr& rw = writer.root();
      ProductionPtr backup = ValidatingGrammarGenerator::doGenerate(rw, m2);
      fixup(backup, m2);

      map<NodePair, ProductionPtr> m;
      ProductionPtr main = doGenerate2(rw, rr, m, m2);
      fixup(main, m);
      return Symbol::rootSymbol(main, backup);
    }

    int ResolvingGrammarGenerator::bestBranch(const NodePtr& writer,
      const NodePtr& reader) {
      Type t = writer->type();

      const size_t c = reader->leaves();
      for (size_t j = 0; j < c; ++j) {
        NodePtr r = reader->leafAt(j);
        if (r->type() == Type::AVRO_SYMBOLIC) {
          r = resolveSymbol(r);
        }
        if (t == r->type()) {
          if (r->hasName()) {
            if (r->name() == writer->name()) {
              return j;
            }
          } else {
            return j;
          }
        }
      }

      for (size_t j = 0; j < c; ++j) {
        const NodePtr& r = reader->leafAt(j);
        Type rt = r->type();
        switch (t) {
          case Type::AVRO_INT:
            if (rt == Type::AVRO_LONG || rt == Type::AVRO_DOUBLE || rt == Type::AVRO_FLOAT) {
              return j;
            }
            break;
          case Type::AVRO_LONG:
          case Type::AVRO_FLOAT:
            if (rt == Type::AVRO_DOUBLE) {
              return j;
            }
            break;
          default:
            break;
        }
      }
      return -1;
    }

    static std::shared_ptr<vector<uint8_t> > getAvroBinary(
      const GenericDatum& defaultValue) {
      EncoderPtr e = binaryEncoder();
      std::shared_ptr<OutputStream> os = memoryOutputStream();
      e->init(*os);
      GenericWriter::write(*e, defaultValue);
      e->flush();
      return snapshot(*os);
    }

    template<typename T1, typename T2>
    struct equalsFirst {
      const T1& v_;

      equalsFirst(const T1& v) : v_(v) {
      }

      bool operator()(const pair<T1, T2>& p) {
        return p.first == v_;
      }
    };

    ProductionPtr ResolvingGrammarGenerator::getWriterProduction(
      const NodePtr& n, map<NodePtr, ProductionPtr>& m2) {
      const NodePtr& nn = (n->type() == Type::AVRO_SYMBOLIC) ?
        static_cast<const NodeSymbolic&> (*n).getNode() : n;
      map<NodePtr, ProductionPtr>::const_iterator it2 = m2.find(nn);
      if (it2 != m2.end()) {
        return it2->second;
      } else {
        ProductionPtr result = ValidatingGrammarGenerator::doGenerate(nn, m2);
        fixup(result, m2);
        return result;
      }
    }

    ProductionPtr ResolvingGrammarGenerator::resolveRecords(
      const NodePtr& writer, const NodePtr& reader,
      map<NodePair, ProductionPtr>& m,
      map<NodePtr, ProductionPtr>& m2) {
      ProductionPtr result = std::make_shared<Production>();

      vector<pair<string, size_t> > wf = fields(writer);
      vector<pair<string, size_t> > rf = fields(reader);
      vector<size_t> fieldOrder;
      fieldOrder.reserve(reader->names());

      /*
       * We look for all writer fields in the reader. If found, recursively
       * resolve the corresponding fields. Then erase the reader field.
       * If no matching field is found for reader, arrange to skip the writer
       * field.
       */
      for (vector<pair<string, size_t> >::const_iterator it = wf.begin();
        it != wf.end(); ++it) {
        vector<pair<string, size_t> >::iterator it2 =
          find_if(rf.begin(), rf.end(),
          equalsFirst<string, size_t>(it->first));
        if (it2 != rf.end()) {
          ProductionPtr p = doGenerate2(writer->leafAt(it->second),
            reader->leafAt(it2->second), m, m2);
          copy(p->rbegin(), p->rend(), back_inserter(*result));
          fieldOrder.push_back(it2->second);
          rf.erase(it2);
        } else {
          ProductionPtr p = getWriterProduction(
            writer->leafAt(it->second), m2);
          result->push_back(Symbol::skipStart());
          if (p->size() == 1) {
            result->push_back((*p)[0]);
          } else {
            result->push_back(Symbol::indirect(p));
          }
        }
      }

      /*
       * Examine the reader fields left out, (i.e. those didn't have corresponding
       * writer field).
       */
      for (vector<pair<string, size_t> >::const_iterator it = rf.begin();
        it != rf.end(); ++it) {

        NodePtr s = reader->leafAt(it->second);
        fieldOrder.push_back(it->second);

        if (s->type() == Type::AVRO_SYMBOLIC) {
          s = resolveSymbol(s);
        }
        std::shared_ptr<vector<uint8_t> > defaultBinary =
          getAvroBinary(reader->defaultValueAt(it->second));
        result->push_back(Symbol::defaultStartAction(defaultBinary));
        map<NodePair, std::shared_ptr<Production> >::const_iterator it2 =
          m.find(NodePair(s, s));
        ProductionPtr p = (it2 == m.end()) ?
          doGenerate2(s, s, m, m2) : it2->second;
        copy(p->rbegin(), p->rend(), back_inserter(*result));
        result->push_back(Symbol::defaultEndAction());
      }
      reverse(result->begin(), result->end());
      result->push_back(Symbol::sizeListAction(fieldOrder));
      result->push_back(Symbol::recordAction());

      return result;

    }

    ProductionPtr ResolvingGrammarGenerator::resolveUnion(
      const NodePtr& writer, const NodePtr& reader,
      map<NodePair, ProductionPtr>& m,
      map<NodePtr, ProductionPtr>& m2) {
      vector<ProductionPtr> v;
      size_t c = writer->leaves();
      v.reserve(c);
      for (size_t i = 0; i < c; ++i) {
        ProductionPtr p = doGenerate2(writer->leafAt(i), reader, m, m2);
        v.push_back(p);
      }
      ProductionPtr result = std::make_shared<Production>();
      result->push_back(Symbol::alternative(v));
      result->push_back(Symbol::writerUnionAction());
      return result;
    }

    ProductionPtr ResolvingGrammarGenerator::doGenerate2(
      const NodePtr& w, const NodePtr& r,
      map<NodePair, ProductionPtr> &m,
      map<NodePtr, ProductionPtr> &m2) {
      const NodePtr writer = w->type() == Type::AVRO_SYMBOLIC ? resolveSymbol(w) : w;
      const NodePtr reader = r->type() == Type::AVRO_SYMBOLIC ? resolveSymbol(r) : r;
      Type writerType = writer->type();
      Type readerType = reader->type();

      if (writerType == readerType) {
        switch (writerType) {
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
          case Type::AVRO_FIXED:
            if (writer->name() == reader->name() &&
              writer->fixedSize() == reader->fixedSize()) {
              ProductionPtr result = std::make_shared<Production>();
              result->push_back(Symbol::sizeCheckSymbol(reader->fixedSize()));
              result->push_back(Symbol::fixedSymbol());
              m[make_pair(writer, reader)] = result;
              return result;
            }
            break;
          case Type::AVRO_RECORD:
            if (writer->name() == reader->name()) {
              const pair<NodePtr, NodePtr> key(writer, reader);
              map<NodePair, ProductionPtr>::const_iterator kp = m.find(key);
              if (kp != m.end()) {
                return (kp->second) ? kp->second :
                  std::make_shared<Production>(1, Symbol::placeholder(key));
              }
              m[key] = ProductionPtr();
              ProductionPtr result = resolveRecords(writer, reader, m, m2);
              m[key] = result;
              return result;
            }
            break;

          case Type::AVRO_ENUM:
            if (writer->name() == reader->name()) {
              ProductionPtr result = std::make_shared<Production>();
              result->push_back(Symbol::enumAdjustSymbol(writer, reader));
              result->push_back(Symbol::enumSymbol());
              m[make_pair(writer, reader)] = result;
              return result;
            }
            break;

          case Type::AVRO_ARRAY:
          {
            ProductionPtr p = getWriterProduction(writer->leafAt(0), m2);
            ProductionPtr p2 = doGenerate2(writer->leafAt(0), reader->leafAt(0), m, m2);
            ProductionPtr result = std::make_shared<Production>();
            result->push_back(Symbol::arrayEndSymbol());
            result->push_back(Symbol::repeater(p2, p, true));
            result->push_back(Symbol::arrayStartSymbol());
            return result;
          }
          case Type::AVRO_MAP:
          {
            ProductionPtr pp =
              doGenerate2(writer->leafAt(1), reader->leafAt(1), m, m2);
            ProductionPtr v(new Production(*pp));
            v->push_back(Symbol::stringSymbol());

            ProductionPtr pp2 = getWriterProduction(writer->leafAt(1), m2);
            ProductionPtr v2(new Production(*pp2));

            v2->push_back(Symbol::stringSymbol());

            ProductionPtr result = std::make_shared<Production>();
            result->push_back(Symbol::mapEndSymbol());
            result->push_back(Symbol::repeater(v, v2, false));
            result->push_back(Symbol::mapStartSymbol());
            return result;
          }
          case Type::AVRO_UNION:
            return resolveUnion(writer, reader, m, m2);
          case Type::AVRO_SYMBOLIC:
          {
            std::shared_ptr<NodeSymbolic> w =
              static_pointer_cast<NodeSymbolic>(writer);
            std::shared_ptr<NodeSymbolic> r =
              static_pointer_cast<NodeSymbolic>(reader);
            NodePair p(w->getNode(), r->getNode());
            map<NodePair, ProductionPtr>::iterator it = m.find(p);
            if (it != m.end() && it->second) {
              return it->second;
            } else {
              m[p] = ProductionPtr();
              return std::make_shared<Production>(1, Symbol::placeholder(p));
            }
          }
          default:
            throw Exception("Unknown node type");
        }
      } else if (writerType == Type::AVRO_UNION) {
        return resolveUnion(writer, reader, m, m2);
      } else {
        switch (readerType) {
          case Type::AVRO_LONG:
            if (writerType == Type::AVRO_INT) {
              return std::make_shared<Production>(1,
                Symbol::resolveSymbol(Symbol::sInt, Symbol::sLong));
            }
            break;
          case Type::AVRO_FLOAT:
            if (writerType == Type::AVRO_INT || writerType == Type::AVRO_LONG) {
              return std::make_shared<Production>(1,
                Symbol::resolveSymbol(writerType == Type::AVRO_INT ?
                Symbol::sInt : Symbol::sLong, Symbol::sFloat));
            }
            break;
          case Type::AVRO_DOUBLE:
            if (writerType == Type::AVRO_INT || writerType == Type::AVRO_LONG
              || writerType == Type::AVRO_FLOAT) {
              return std::make_shared<Production>(1,
                Symbol::resolveSymbol(writerType == Type::AVRO_INT ?
                Symbol::sInt : writerType == Type::AVRO_LONG ?
                Symbol::sLong : Symbol::sFloat, Symbol::sDouble));
            }
            break;

          case Type::AVRO_UNION:
          {
            int j = bestBranch(writer, reader);
            if (j >= 0) {
              ProductionPtr p = doGenerate2(writer, reader->leafAt(j), m, m2);
              ProductionPtr result = std::make_shared<Production>();
              result->push_back(Symbol::unionAdjustSymbol(j, p));
              result->push_back(Symbol::unionSymbol());
              return result;
            }
          }
            break;
          case Type::AVRO_NULL:
          case Type::AVRO_BOOL:
          case Type::AVRO_INT:
          case Type::AVRO_STRING:
          case Type::AVRO_BYTES:
          case Type::AVRO_ENUM:
          case Type::AVRO_ARRAY:
          case Type::AVRO_MAP:
          case Type::AVRO_RECORD:
            break;
          default:
            throw Exception("Unknown node type");
        }
      }
      return std::make_shared<Production>(1, Symbol::error(writer, reader));
    }

    class ResolvingDecoderHandler {
      std::shared_ptr<vector<uint8_t> > defaultData_;
      std::shared_ptr<InputStream> inp_;
      DecoderPtr backup_;
      DecoderPtr& base_;
      const DecoderPtr binDecoder;
    public:

      ResolvingDecoderHandler(DecoderPtr& base) : base_(base),
      binDecoder(binaryDecoder()) {
      }

      size_t handle(const Symbol& s) {
        switch (s.kind()) {
          case Symbol::sWriterUnion:
            return base_->decodeUnionIndex();
          case Symbol::sDefaultStart:
            defaultData_ = s.extra<std::shared_ptr<vector<uint8_t> > >();
            backup_ = base_;
            inp_ = memoryInputStream(&(*defaultData_)[0], defaultData_->size());
            base_ = binDecoder;
            base_->init(*inp_);
            return 0;
          case Symbol::sDefaultEnd:
            base_ = backup_;
            backup_.reset();
            return 0;
          default:
            return 0;
        }
      }

      void reset() {
        if (backup_ != NULL) {
          base_ = backup_;
          backup_.reset();
        }
      }
    };

    template <typename Parser>
    class ResolvingDecoderImpl : public ResolvingDecoder {
      DecoderPtr base_;
      ResolvingDecoderHandler handler_;
      Parser parser_;

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
      const vector<size_t>& fieldOrder();
    public:

      ResolvingDecoderImpl(const ValidSchema& writer, const ValidSchema& reader,
        const DecoderPtr& base) :
      base_(base),
      handler_(base_),
      parser_(ResolvingGrammarGenerator().generate(writer, reader),
      &(*base_), handler_) {
      }
    };

    template <typename P>
    void ResolvingDecoderImpl<P>::init(InputStream& is) {
      handler_.reset();
      base_->init(is);
      parser_.reset();
    }

    template <typename P>
    void ResolvingDecoderImpl<P>::decodeNull() {
      parser_.advance(Symbol::sNull);
      base_->decodeNull();
    }

    template <typename P>
    bool ResolvingDecoderImpl<P>::decodeBool() {
      parser_.advance(Symbol::sBool);
      return base_->decodeBool();
    }

    template <typename P>
    int32_t ResolvingDecoderImpl<P>::decodeInt() {
      parser_.advance(Symbol::sInt);
      return base_->decodeInt();
    }

    template <typename P>
    int64_t ResolvingDecoderImpl<P>::decodeLong() {
      Symbol::Kind k = parser_.advance(Symbol::sLong);
      return k == Symbol::sInt ? base_->decodeInt() : base_->decodeLong();
    }

    template <typename P>
    float ResolvingDecoderImpl<P>::decodeFloat() {
      Symbol::Kind k = parser_.advance(Symbol::sFloat);
      return k == Symbol::sInt ? base_->decodeInt() :
        k == Symbol::sLong ? base_->decodeLong() :
        base_->decodeFloat();
    }

    template <typename P>
    double ResolvingDecoderImpl<P>::decodeDouble() {
      Symbol::Kind k = parser_.advance(Symbol::sDouble);
      return k == Symbol::sInt ? base_->decodeInt() :
        k == Symbol::sLong ? base_->decodeLong() :
        k == Symbol::sFloat ? base_->decodeFloat() :
        base_->decodeDouble();
    }

    template <typename P>
    void ResolvingDecoderImpl<P>::decodeString(string& value) {
      parser_.advance(Symbol::sString);
      base_->decodeString(value);
    }

    template <typename P>
    void ResolvingDecoderImpl<P>::skipString() {
      parser_.advance(Symbol::sString);
      base_->skipString();
    }

    template <typename P>
    void ResolvingDecoderImpl<P>::decodeBytes(vector<uint8_t>& value) {
      parser_.advance(Symbol::sBytes);
      base_->decodeBytes(value);
    }

    template <typename P>
    void ResolvingDecoderImpl<P>::skipBytes() {
      parser_.advance(Symbol::sBytes);
      base_->skipBytes();
    }

    template <typename P>
    void ResolvingDecoderImpl<P>::decodeFixed(size_t n, vector<uint8_t>& value) {
      parser_.advance(Symbol::sFixed);
      parser_.assertSize(n);
      return base_->decodeFixed(n, value);
    }

    template <typename P>
    void ResolvingDecoderImpl<P>::skipFixed(size_t n) {
      parser_.advance(Symbol::sFixed);
      parser_.assertSize(n);
      base_->skipFixed(n);
    }

    template <typename P>
    size_t ResolvingDecoderImpl<P>::decodeEnum() {
      parser_.advance(Symbol::sEnum);
      size_t n = base_->decodeEnum();
      return parser_.enumAdjust(n);
    }

    template <typename P>
    size_t ResolvingDecoderImpl<P>::arrayStart() {
      parser_.advance(Symbol::sArrayStart);
      size_t result = base_->arrayStart();
      if (result == 0) {
        parser_.popRepeater();
        parser_.advance(Symbol::sArrayEnd);
      } else {
        parser_.setRepeatCount(result);
      }
      return result;
    }

    template <typename P>
    size_t ResolvingDecoderImpl<P>::arrayNext() {
      parser_.processImplicitActions();
      size_t result = base_->arrayNext();
      if (result == 0) {
        parser_.popRepeater();
        parser_.advance(Symbol::sArrayEnd);
      } else {
        parser_.setRepeatCount(result);
      }
      return result;
    }

    template <typename P>
    size_t ResolvingDecoderImpl<P>::skipArray() {
      parser_.advance(Symbol::sArrayStart);
      size_t n = base_->skipArray();
      if (n == 0) {
        parser_.pop();
      } else {
        parser_.setRepeatCount(n);
        parser_.skip(*base_);
      }
      parser_.advance(Symbol::sArrayEnd);
      return 0;
    }

    template <typename P>
    size_t ResolvingDecoderImpl<P>::mapStart() {
      parser_.advance(Symbol::sMapStart);
      size_t result = base_->mapStart();
      if (result == 0) {
        parser_.popRepeater();
        parser_.advance(Symbol::sMapEnd);
      } else {
        parser_.setRepeatCount(result);
      }
      return result;
    }

    template <typename P>
    size_t ResolvingDecoderImpl<P>::mapNext() {
      parser_.processImplicitActions();
      size_t result = base_->mapNext();
      if (result == 0) {
        parser_.popRepeater();
        parser_.advance(Symbol::sMapEnd);
      } else {
        parser_.setRepeatCount(result);
      }
      return result;
    }

    template <typename P>
    size_t ResolvingDecoderImpl<P>::skipMap() {
      parser_.advance(Symbol::sMapStart);
      size_t n = base_->skipMap();
      if (n == 0) {
        parser_.pop();
      } else {
        parser_.setRepeatCount(n);
        parser_.skip(*base_);
      }
      parser_.advance(Symbol::sMapEnd);
      return 0;
    }

    template <typename P>
    size_t ResolvingDecoderImpl<P>::decodeUnionIndex() {
      parser_.advance(Symbol::sUnion);
      return parser_.unionAdjust();
    }

    template <typename P>
    const vector<size_t>& ResolvingDecoderImpl<P>::fieldOrder() {
      parser_.advance(Symbol::sRecord);
      return parser_.sizeList();
    }

  } // namespace parsing

  ResolvingDecoderPtr resolvingDecoder(const ValidSchema& writer,
    const ValidSchema& reader, const DecoderPtr& base) {
    return std::make_shared<parsing::ResolvingDecoderImpl
      <parsing::SimpleParser<parsing::ResolvingDecoderHandler> > >(
      writer, reader, base);
  }

}

