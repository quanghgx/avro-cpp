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

#ifndef avro_Layout_hh__
#define avro_Layout_hh__

#include <boost/ptr_container/ptr_vector.hpp>

/// \file Layout.hh
///

namespace avro {

  class Layout {
  protected:

    Layout(size_t offset = 0) :
    offset_(offset) { }

  public:
    Layout(const Layout&) = delete;
    const Layout& operator=(const Layout&) = delete;

    size_t offset() const {
      return offset_;
    }

    virtual ~Layout() { }

  private:

    const size_t offset_;
  };

  class PrimitiveLayout : public Layout {
  public:

    PrimitiveLayout(size_t offset = 0) :
    Layout(offset) { }
  };

  class CompoundLayout : public Layout {
  public:

    CompoundLayout(size_t offset = 0) :
    Layout(offset) { }

    void add(Layout *layout) {
      layouts_.push_back(layout);
    }

    const Layout &at(size_t idx) const {
      return layouts_.at(idx);
    }

  private:

    boost::ptr_vector<Layout> layouts_;
  };

} // namespace avro

#endif
