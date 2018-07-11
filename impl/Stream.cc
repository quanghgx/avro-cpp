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

#include "Stream.hh"
#include <vector>

namespace avro {

  using std::vector;

  class MemoryInputStream : public InputStream {
    const std::vector<uint8_t*>& m_data;
    const size_t m_chunk_size;
    const size_t m_size;
    const size_t m_available;
    size_t m_cur;
    size_t m_cur_len;

    size_t maxLen() {
      size_t n = (m_cur == (m_size - 1)) ? m_available : m_chunk_size;
      if (n == m_cur_len) {
        if (m_cur == (m_size - 1)) {
          return 0;
        }
        ++m_cur;
        n = (m_cur == (m_size - 1)) ? m_available : m_chunk_size;
        m_cur_len = 0;
      }
      return n;
    }

  public:

    MemoryInputStream(const std::vector<uint8_t*>& b,
      size_t chunkSize, size_t available) :
    m_data(b), m_chunk_size(chunkSize), m_size(b.size()),
    m_available(available), m_cur(0), m_cur_len(0) {
    }

    bool next(const uint8_t** data, size_t* len) override {
      if (size_t n = maxLen()) {
        *data = m_data[m_cur] + m_cur_len;
        *len = n - m_cur_len;
        m_cur_len = n;
        return true;
      }
      return false;
    }

    void backup(size_t len) override {
      m_cur_len -= len;
    }

    void skip(size_t len) override {
      while (len > 0) {
        if (size_t n = maxLen()) {
          if ((m_cur_len + len) < n) {
            n = m_cur_len + len;
          }
          len -= n - m_cur_len;
          m_cur_len = n;
        } else {
          break;
        }
      }
    }

    size_t byteCount() const override {
      return m_cur * m_chunk_size + m_cur_len;
    }
  };

  class MemoryInputStream2 : public InputStream {
    const uint8_t * const m_data;
    const size_t m_size;
    size_t m_cur_len;
  public:

    MemoryInputStream2(const uint8_t *data, size_t len)
    : m_data(data), m_size(len), m_cur_len(0) {
    }

    bool next(const uint8_t** data, size_t* len) override {
      if (m_cur_len == m_size) {
        return false;
      }
      *data = &m_data[m_cur_len];
      *len = m_size - m_cur_len;
      m_cur_len = m_size;
      return true;
    }

    void backup(size_t len) override {
      m_cur_len -= len;
    }

    void skip(size_t len) override {
      if (len > (m_size - m_cur_len)) {
        len = m_size - m_cur_len;
      }
      m_cur_len += len;
    }

    size_t byteCount() const override {
      return m_cur_len;
    }
  };

  class MemoryOutputStream : public OutputStream {
  public:
    const size_t m_chunk_size;
    std::vector<uint8_t*> m_data;
    size_t m_available;
    size_t m_byte_count;

    MemoryOutputStream(size_t chunkSize) : m_chunk_size(chunkSize),
    m_available(0), m_byte_count(0) {
    }

    ~MemoryOutputStream() {
      for (std::vector<uint8_t*>::const_iterator it = m_data.begin();
        it != m_data.end(); ++it) {
        delete[] * it;
      }
    }

    bool next(uint8_t** data, size_t* len) {
      if (m_available == 0) {
        m_data.push_back(new uint8_t[m_chunk_size]);
        m_available = m_chunk_size;
      }
      *data = &m_data.back()[m_chunk_size - m_available];
      *len = m_available;
      m_byte_count += m_available;
      m_available = 0;
      return true;
    }

    void backup(size_t len) {
      m_available += len;
      m_byte_count -= len;
    }

    uint64_t byteCount() const {
      return m_byte_count;
    }

    void flush() {
    }
  };

  std::shared_ptr<OutputStream> memoryOutputStream(size_t chunkSize) {
    return std::shared_ptr<OutputStream>(new MemoryOutputStream(chunkSize));
  }

  std::shared_ptr<InputStream> memoryInputStream(const uint8_t* data, size_t len) {
    return std::shared_ptr<InputStream>(new MemoryInputStream2(data, len));
  }

  std::shared_ptr<InputStream> memoryInputStream(const OutputStream& source) {
    const MemoryOutputStream& mos =
      dynamic_cast<const MemoryOutputStream&> (source);
    return (mos.m_data.empty()) ?
      std::shared_ptr<InputStream>(new MemoryInputStream2(0, 0)) :
      std::shared_ptr<InputStream>(new MemoryInputStream(mos.m_data,
      mos.m_chunk_size,
      (mos.m_chunk_size - mos.m_available)));
  }

  std::shared_ptr<std::vector<uint8_t> > snapshot(const OutputStream& source) {
    const MemoryOutputStream& mos = dynamic_cast<const MemoryOutputStream&> (source);
    std::shared_ptr<std::vector<uint8_t> > result(new std::vector<uint8_t>());
    size_t c = mos.m_byte_count;
    result->reserve(mos.m_byte_count);
    for (vector<uint8_t*>::const_iterator it = mos.m_data.begin();
      it != mos.m_data.end(); ++it) {
      size_t n = std::min(c, mos.m_chunk_size);
      std::copy(*it, *it + n, std::back_inserter(*result));
      c -= n;
    }
    return result;
  }

}

