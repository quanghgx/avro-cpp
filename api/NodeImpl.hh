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

#ifndef avro_NodeImpl_hh__
#define avro_NodeImpl_hh__

#include <limits>
#include <set>

#include "GenericDatum.hh"
#include "Node.hh"
#include "NodeConcepts.hh"

namespace avro {

  /* Implementation details for Node.  NodeImpl represents all the avro types,  whose properties are enabled are disabled by selecting concept classes.*/
  template
  <
  class NameConcept,
  class LeavesConcept,
  class LeafNamesConcept,
  class SizeConcept
  >
  class NodeImpl : public Node {
  protected:

    NodeImpl(Type type) :
    Node(type),
    nameAttribute_(),
    leafAttributes_(),
    leafNameAttributes_(),
    sizeAttribute_() { }

    NodeImpl(Type type,
      const NameConcept &name,
      const LeavesConcept &leaves,
      const LeafNamesConcept &leafNames,
      const SizeConcept &size) :
    Node(type),
    nameAttribute_(name),
    leafAttributes_(leaves),
    leafNameAttributes_(leafNames),
    sizeAttribute_(size) { }

    void swap(NodeImpl& impl) {
      std::swap(nameAttribute_, impl.nameAttribute_);
      std::swap(leafAttributes_, impl.leafAttributes_);
      std::swap(leafNameAttributes_, impl.leafNameAttributes_);
      std::swap(sizeAttribute_, impl.sizeAttribute_);
      std::swap(nameIndex_, impl.nameIndex_);
    }

    bool hasName() const {
      return NameConcept::hasAttribute;
    }

    void doSetName(const Name &name) {
      nameAttribute_.add(name);
    }

    const Name &name() const {
      return nameAttribute_.get();
    }

    void doAddLeaf(const NodePtr &newLeaf) {
      leafAttributes_.add(newLeaf);
    }

    size_t leaves() const {
      return leafAttributes_.size();
    }

    const NodePtr &leafAt(int index) const {
      return leafAttributes_.get(index);
    }

    void doAddName(const std::string &name) {
      if (!nameIndex_.add(name, leafNameAttributes_.size())) {
        throw Exception(boost::format("Cannot add duplicate name: %1%") % name);
      }
      leafNameAttributes_.add(name);
    }

    size_t names() const {
      return leafNameAttributes_.size();
    }

    const std::string &nameAt(int index) const {
      return leafNameAttributes_.get(index);
    }

    bool nameIndex(const std::string &name, size_t &index) const {
      return nameIndex_.lookup(name, index);
    }

    void doSetFixedSize(int size) {
      sizeAttribute_.add(size);
    }

    int fixedSize() const {
      return sizeAttribute_.get();
    }

    virtual bool isValid() const = 0;

    void printBasicInfo(std::ostream &os) const;

    void setLeafToSymbolic(int index, const NodePtr &node);

    SchemaResolution furtherResolution(const Node &reader) const {
      SchemaResolution match = SchemaResolution::NO_MATCH;

      if (reader.type() == Type::AVRO_SYMBOLIC) {

        // resolve the symbolic type, and check again
        const NodePtr &node = reader.leafAt(0);
        match = resolve(*node);
      }

      return match;
    }

    NameConcept nameAttribute_;
    LeavesConcept leafAttributes_;
    LeafNamesConcept leafNameAttributes_;
    SizeConcept sizeAttribute_;
    concepts::NameIndexConcept<LeafNamesConcept> nameIndex_;
  };

  typedef concepts::NoAttribute<Name> NoName;
  typedef concepts::SingleAttribute<Name> HasName;

  typedef concepts::NoAttribute<NodePtr> NoLeaves;
  typedef concepts::SingleAttribute<NodePtr> SingleLeaf;
  typedef concepts::MultiAttribute<NodePtr> MultiLeaves;

  typedef concepts::NoAttribute<std::string> NoLeafNames;
  typedef concepts::MultiAttribute<std::string> LeafNames;

  typedef concepts::NoAttribute<int> NoSize;
  typedef concepts::SingleAttribute<int> HasSize;

  typedef NodeImpl< NoName, NoLeaves, NoLeafNames, NoSize > NodeImplPrimitive;
  typedef NodeImpl< HasName, NoLeaves, NoLeafNames, NoSize > NodeImplSymbolic;

  typedef NodeImpl< HasName, MultiLeaves, LeafNames, NoSize > NodeImplRecord;
  typedef NodeImpl< HasName, NoLeaves, LeafNames, NoSize > NodeImplEnum;
  typedef NodeImpl< NoName, SingleLeaf, NoLeafNames, NoSize > NodeImplArray;
  typedef NodeImpl< NoName, MultiLeaves, NoLeafNames, NoSize > NodeImplMap;
  typedef NodeImpl< NoName, MultiLeaves, NoLeafNames, NoSize > NodeImplUnion;
  typedef NodeImpl< HasName, NoLeaves, NoLeafNames, HasSize > NodeImplFixed;

  class NodePrimitive : public NodeImplPrimitive {
  public:

    explicit NodePrimitive(Type type) :
    NodeImplPrimitive(type) { }

    SchemaResolution resolve(const Node &reader) const;

    void printJson(std::ostream &os, int depth) const;

    bool isValid() const {
      return true;
    }
  };

  class NodeSymbolic : public NodeImplSymbolic {
    typedef std::weak_ptr<Node> NodeWeakPtr;

  public:

    NodeSymbolic() :
    NodeImplSymbolic(Type::AVRO_SYMBOLIC) { }

    explicit NodeSymbolic(const HasName &name) :
    NodeImplSymbolic(Type::AVRO_SYMBOLIC, name, NoLeaves(), NoLeafNames(), NoSize()) { }

    NodeSymbolic(const HasName &name, const NodePtr n) :
    NodeImplSymbolic(Type::AVRO_SYMBOLIC, name, NoLeaves(), NoLeafNames(), NoSize()), actualNode_(n) { }
    SchemaResolution resolve(const Node &reader) const;

    void printJson(std::ostream &os, int depth) const;

    bool isValid() const {
      return (nameAttribute_.size() == 1);
    }

    bool isSet() const {
      return (actualNode_.lock() != 0);
    }

    NodePtr getNode() const {
      NodePtr node = actualNode_.lock();
      if (!node) {
        throw Exception(boost::format("Could not follow symbol %1%") % name());
      }
      return node;
    }

    void setNode(const NodePtr &node) {
      actualNode_ = node;
    }

  protected:

    NodeWeakPtr actualNode_;

  };

  class NodeRecord : public NodeImplRecord {
    std::vector<GenericDatum> defaultValues;
  public:

    NodeRecord() : NodeImplRecord(Type::AVRO_RECORD) { }

    NodeRecord(const HasName &name, const MultiLeaves &fields,
      const LeafNames &fieldsNames,
      const std::vector<GenericDatum>& dv) :
    NodeImplRecord(Type::AVRO_RECORD, name, fields, fieldsNames, NoSize()),
    defaultValues(dv) {
      for (size_t i = 0; i < leafNameAttributes_.size(); ++i) {
        if (!nameIndex_.add(leafNameAttributes_.get(i), i)) {
          throw Exception(boost::format(
            "Cannot add duplicate name: %1%") %
            leafNameAttributes_.get(i));
        }
      }
    }

    void swap(NodeRecord& r) {
      NodeImplRecord::swap(r);
      defaultValues.swap(r.defaultValues);
    }

    SchemaResolution resolve(const Node &reader) const;

    void printJson(std::ostream &os, int depth) const;

    bool isValid() const {
      return ((nameAttribute_.size() == 1) &&
        (leafAttributes_.size() == leafNameAttributes_.size()));
    }

    const GenericDatum& defaultValueAt(int index) {
      return defaultValues[index];
    }
  };
   

  template < class A, class B, class C, class D >
  inline void NodeImpl<A, B, C, D>::setLeafToSymbolic(int index, const NodePtr &node) {
    if (!B::hasAttribute) {
      throw Exception("Cannot change leaf node for nonexistent leaf");
    }

    NodePtr &replaceNode = const_cast<NodePtr &> (leafAttributes_.get(index));
    if (replaceNode->name() != node->name()) {
      throw Exception("Symbolic name does not match the name of the schema it references");
    }

    NodePtr symbol(new NodeSymbolic);
    NodeSymbolic *ptr = static_cast<NodeSymbolic *> (symbol.get());

    ptr->setName(node->name());
    ptr->setNode(node);
    replaceNode.swap(symbol);
  }

  template < class A, class B, class C, class D >
  inline void NodeImpl<A, B, C, D>::printBasicInfo(std::ostream &os) const {
    os << type();
    if (hasName()) {
      os << ' ' << nameAttribute_.get();
    }

    if (D::hasAttribute) {
      os << " " << sizeAttribute_.get();
    }
    os << '\n';
    int count = leaves();
    count = count ? count : names();
    for (int i = 0; i < count; ++i) {
      if (C::hasAttribute) {
        os << "name " << nameAt(i) << '\n';
      }
      if (type() != Type::AVRO_SYMBOLIC && leafAttributes_.hasAttribute) {
        leafAt(i)->printBasicInfo(os);
      }
    }
    if (isCompound(type())) {
      os << "end " << type() << '\n';
    }
  }

  inline NodePtr resolveSymbol(const NodePtr &node) {
    if (node->type() != Type::AVRO_SYMBOLIC) {
      throw Exception("Only symbolic nodes may be resolved");
    }
    std::shared_ptr<NodeSymbolic> symNode = std::static_pointer_cast<NodeSymbolic>(node);
    return symNode->getNode();
  }

}

#endif
