
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

#include "boost/scoped_array.hpp"
#include "Resolver.hh"
#include "Layout.hh"
#include "NodeImpl.hh"
#include "ValidSchema.hh"
#include "Reader.hh"
#include "AvroTraits.hh"

namespace avro {

  class ResolverFactory;
  typedef std::shared_ptr<Resolver> ResolverPtr;
  typedef boost::ptr_vector<Resolver> ResolverPtrVector;


#ifdef DEBUG_VERBOSE
#define DEBUG_OUT(str) std::cout << str << '\n'
#else

  class NoOp {
  };

  template<typename T> NoOp& operator<<(NoOp &noOp, const T&) {
    return noOp;
  }
  NoOp noop;
#define DEBUG_OUT(str) noop << str 
#endif

  template<typename T>
  class PrimitiveSkipper : public Resolver {
  public:

    PrimitiveSkipper() :
    Resolver() {
    }

    virtual void parse(Reader &reader, uint8_t *address) const {
      T val;
      reader.readValue(val);
      DEBUG_OUT("Skipping " << val);
    }
  };

  template<typename T>
  class PrimitiveParser : public Resolver {
  public:

    PrimitiveParser(const PrimitiveLayout &offset) :
    Resolver(),
    offset_(offset.offset()) {
    }

    virtual void parse(Reader &reader, uint8_t *address) const {
      T* location = reinterpret_cast<T *> (address + offset_);
      reader.readValue(*location);
      DEBUG_OUT("Reading " << *location);
    }

  private:

    size_t offset_;
  };

  template<typename WT, typename RT>
  class PrimitivePromoter : public Resolver {
  public:

    PrimitivePromoter(const PrimitiveLayout &offset) :
    Resolver(),
    offset_(offset.offset()) {
    }

    virtual void parse(Reader &reader, uint8_t *address) const {
      parseIt<WT>(reader, address);
    }

  private:

    void parseIt(Reader &reader, uint8_t *address, const boost::true_type &) const {
      WT val;
      reader.readValue(val);
      RT *location = reinterpret_cast<RT *> (address + offset_);
      *location = static_cast<RT> (val);
      DEBUG_OUT("Promoting " << val);
    }

    void parseIt(Reader &reader, uint8_t *address, const boost::false_type &) const {
    }

    template<typename T>
    void parseIt(Reader &reader, uint8_t *address) const {
      parseIt(reader, address, is_promotable<T>());
    }

    size_t offset_;
  };

  template <>
  class PrimitiveSkipper<std::vector<uint8_t> > : public Resolver {
  public:

    PrimitiveSkipper() :
    Resolver() {
    }

    virtual void parse(Reader &reader, uint8_t *address) const {
      std::vector<uint8_t> val;
      reader.readBytes(val);
      DEBUG_OUT("Skipping bytes");
    }
  };

  template <>
  class PrimitiveParser<std::vector<uint8_t> > : public Resolver {
  public:

    PrimitiveParser(const PrimitiveLayout &offset) :
    Resolver(),
    offset_(offset.offset()) {
    }

    virtual void parse(Reader &reader, uint8_t *address) const {
      std::vector<uint8_t> *location = reinterpret_cast<std::vector<uint8_t> *> (address + offset_);
      reader.readBytes(*location);
      DEBUG_OUT("Reading bytes");
    }

  private:

    size_t offset_;
  };

  class RecordSkipper : public Resolver {
  public:

    RecordSkipper(ResolverFactory &factory, const NodePtr &writer);

    virtual void parse(Reader &reader, uint8_t *address) const {
      DEBUG_OUT("Skipping record");

      reader.readRecord();
      size_t steps = resolvers_.size();
      for (size_t i = 0; i < steps; ++i) {
        resolvers_[i].parse(reader, address);
      }
    }

  protected:

    ResolverPtrVector resolvers_;

  };

  class RecordParser : public Resolver {
  public:

    virtual void parse(Reader &reader, uint8_t *address) const {
      DEBUG_OUT("Reading record");

      reader.readRecord();
      size_t steps = resolvers_.size();
      for (size_t i = 0; i < steps; ++i) {
        resolvers_[i].parse(reader, address);
      }
    }

    RecordParser(ResolverFactory &factory, const NodePtr &writer, const NodePtr &reader, const CompoundLayout &offsets);

  protected:

    ResolverPtrVector resolvers_;

  };

  class ResolverFactory {

    template<typename T>
    Resolver*
    constructPrimitiveSkipper(const NodePtr &writer) {
      return new PrimitiveSkipper<T>();
    }

    template<typename T>
    Resolver*
    constructPrimitive(const NodePtr &writer, const NodePtr &reader, const Layout &offset) {
      Resolver *instruction = 0;

      SchemaResolution match = writer->resolve(*reader);

      if (match == SchemaResolution::NO_MATCH) {
        instruction = new PrimitiveSkipper<T>();
      } else if (match == SchemaResolution::MATCH) {
        const PrimitiveLayout &primitiveLayout = static_cast<const PrimitiveLayout &> (offset);
        instruction = new PrimitiveParser<T>(primitiveLayout);
      } else if (match == SchemaResolution::PROMOTABLE_TO_LONG) {
        const PrimitiveLayout &primitiveLayout = static_cast<const PrimitiveLayout &> (offset);
        instruction = new PrimitivePromoter<T, int64_t>(primitiveLayout);
      } else if (match == SchemaResolution::PROMOTABLE_TO_FLOAT) {
        const PrimitiveLayout &primitiveLayout = static_cast<const PrimitiveLayout &> (offset);
        instruction = new PrimitivePromoter<T, float>(primitiveLayout);
      } else if (match == SchemaResolution::PROMOTABLE_TO_DOUBLE) {
        const PrimitiveLayout &primitiveLayout = static_cast<const PrimitiveLayout &> (offset);
        instruction = new PrimitivePromoter<T, double>(primitiveLayout);
      } else {
        assert(0);
      }
      return instruction;
    }

    template<typename Skipper>
    Resolver*
    constructCompoundSkipper(const NodePtr &writer) {
      return new Skipper(*this, writer);
    }

    template<typename Parser, typename Skipper>
    Resolver*
    constructCompound(const NodePtr &writer, const NodePtr &reader, const Layout &offset) {
      Resolver *instruction;

      SchemaResolution match = SchemaResolution::NO_MATCH;

      match = writer->resolve(*reader);

      if (match == SchemaResolution::NO_MATCH) {
        instruction = new Skipper(*this, writer);
      } else {
        const CompoundLayout &compoundLayout = dynamic_cast<const CompoundLayout &> (offset);
        instruction = new Parser(*this, writer, reader, compoundLayout);
      }

      return instruction;
    }

  public:

    ResolverFactory(const ResolverFactory&) = delete;
    const ResolverFactory& operator=(const ResolverFactory&) = delete;

    ResolverFactory() {
    }

    Resolver *
    construct(const NodePtr &writer, const NodePtr &reader, const Layout &offset) {

      typedef Resolver * (ResolverFactory::*BuilderFunc)(const NodePtr &writer, const NodePtr &reader, const Layout & offset);

      NodePtr currentWriter = (writer->type() == Type::AVRO_SYMBOLIC) ?
        resolveSymbol(writer) :
        writer;

      NodePtr currentReader = (reader->type() == Type::AVRO_SYMBOLIC) ?
        resolveSymbol(reader) :
        reader;

      static const BuilderFunc funcs[] = {
        &ResolverFactory::constructPrimitive<std::string>,
        &ResolverFactory::constructPrimitive<std::vector<uint8_t> >,
        &ResolverFactory::constructPrimitive<int32_t>,
        &ResolverFactory::constructPrimitive<int64_t>,
        &ResolverFactory::constructPrimitive<float>,
        &ResolverFactory::constructPrimitive<double>,
        &ResolverFactory::constructPrimitive<bool>,
        &ResolverFactory::constructPrimitive<Null>,
        &ResolverFactory::constructCompound<RecordParser, RecordSkipper>
      };

      //static_assert((sizeof (funcs) / sizeof (BuilderFunc)) == type_as_integer(Type::AVRO_NUM_TYPES));

      BuilderFunc func = funcs[type_as_integer(currentWriter->type())];
      assert(func);

      return ((this)->*(func))(currentWriter, currentReader, offset);
    }

    Resolver *
    skipper(const NodePtr &writer) {

      typedef Resolver * (ResolverFactory::*BuilderFunc)(const NodePtr & writer);

      NodePtr currentWriter = (writer->type() == Type::AVRO_SYMBOLIC) ?
        writer->leafAt(0) : writer;

      static const BuilderFunc funcs[] = {
        &ResolverFactory::constructPrimitiveSkipper<std::string>,
        &ResolverFactory::constructPrimitiveSkipper<std::vector<uint8_t> >,
        &ResolverFactory::constructPrimitiveSkipper<int32_t>,
        &ResolverFactory::constructPrimitiveSkipper<int64_t>,
        &ResolverFactory::constructPrimitiveSkipper<float>,
        &ResolverFactory::constructPrimitiveSkipper<double>,
        &ResolverFactory::constructPrimitiveSkipper<bool>,
        &ResolverFactory::constructPrimitiveSkipper<Null>,
        &ResolverFactory::constructCompoundSkipper<RecordSkipper>
      };

      //static_assert((sizeof (funcs) / sizeof (BuilderFunc)) == type_as_integer(Type::AVRO_NUM_TYPES));

      BuilderFunc func = funcs[type_as_integer(currentWriter->type())];
      assert(func);

      return ((this)->*(func))(currentWriter);
    }
  };

  RecordSkipper::RecordSkipper(ResolverFactory &factory, const NodePtr &writer) :
  Resolver() {
    size_t leaves = writer->leaves();
    resolvers_.reserve(leaves);
    for (size_t i = 0; i < leaves; ++i) {
      const NodePtr &w = writer->leafAt(i);
      resolvers_.push_back(factory.skipper(w));
    }
  }

  RecordParser::RecordParser(ResolverFactory &factory, const NodePtr &writer, const NodePtr &reader, const CompoundLayout &offsets) :
  Resolver() {
    size_t leaves = writer->leaves();
    resolvers_.reserve(leaves);
    for (size_t i = 0; i < leaves; ++i) {

      const NodePtr &w = writer->leafAt(i);

      const std::string &name = writer->nameAt(i);

      size_t readerIndex = 0;
      bool found = reader->nameIndex(name, readerIndex);

      if (found) {
        const NodePtr &r = reader->leafAt(readerIndex);
        resolvers_.push_back(factory.construct(w, r, offsets.at(readerIndex)));
      } else {
        resolvers_.push_back(factory.skipper(w));
      }
    }
  }
 

  Resolver *constructResolver(const ValidSchema &writerSchema,
    const ValidSchema &readerSchema,
    const Layout &readerLayout) {
    ResolverFactory factory;
    return factory.construct(writerSchema.root(), readerSchema.root(), readerLayout);
  }

}
