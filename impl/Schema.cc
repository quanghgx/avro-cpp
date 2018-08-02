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


#include "Schema.hh"

namespace avro {

  Schema::Schema() {
  }

  Schema::~Schema() {
  }

  Schema::Schema(const NodePtr &node) :
  node_(node) {
  }

  Schema::Schema(Node *node) :
  node_(node) {
  }

  RecordSchema::RecordSchema(const std::string &name) :
  Schema(new NodeRecord) {
    node_->setName(name);
  }

  void
  RecordSchema::addField(const std::string &name, const Schema &fieldSchema) {
    // add the name first. it will throw if the name is a duplicate, preventing
    // the leaf from being added
    node_->addName(name);

    node_->addLeaf(fieldSchema.root());
  }  

  SymbolicSchema::SymbolicSchema(const Name &name, const NodePtr& link) :
  Schema(new NodeSymbolic(HasName(name), link)) {
  }

}
