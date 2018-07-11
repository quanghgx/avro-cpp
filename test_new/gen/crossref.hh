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


#ifndef CROSSREF_HH_2542349456__H_
#define CROSSREF_HH_2542349456__H_


#include <sstream>
#include "boost/any.hpp"
#include "Specific.hh"
#include "Encoder.hh"
#include "Decoder.hh"

namespace cr {
struct B;
struct A {
    std::vector<B > edges;
    A() :
        edges(std::vector<B >())
        { }
};

struct C {
    std::map<std::string, A > x;
    C() :
        x(std::map<std::string, A >())
        { }
};

struct _crossref_Union__0__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    C get_C() const;
    void set_C(const C& v);
    int32_t get_int() const;
    void set_int(const int32_t& v);
    _crossref_Union__0__();
};

struct B {
    _crossref_Union__0__ child;
    B() :
        child(_crossref_Union__0__())
        { }
};

inline
C _crossref_Union__0__::get_C() const {
    if (idx_ != 0) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<C >(value_);
}

inline
void _crossref_Union__0__::set_C(const C& v) {
    idx_ = 0;
    value_ = v;
}

inline
int32_t _crossref_Union__0__::get_int() const {
    if (idx_ != 1) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<int32_t >(value_);
}

inline
void _crossref_Union__0__::set_int(const int32_t& v) {
    idx_ = 1;
    value_ = v;
}

inline _crossref_Union__0__::_crossref_Union__0__() : idx_(0), value_(C()) { }
}
namespace avro {
template<> struct codec_traits<cr::C> {
    static void encode(Encoder& e, const cr::C& v) {
        avro::encode(e, v.x);
    }
    static void decode(Decoder& d, cr::C& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.x);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.x);
        }
    }
};

template<> struct codec_traits<cr::_crossref_Union__0__> {
    static void encode(Encoder& e, cr::_crossref_Union__0__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            avro::encode(e, v.get_C());
            break;
        case 1:
            avro::encode(e, v.get_int());
            break;
        }
    }
    static void decode(Decoder& d, cr::_crossref_Union__0__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            {
                cr::C vv;
                avro::decode(d, vv);
                v.set_C(vv);
            }
            break;
        case 1:
            {
                int32_t vv;
                avro::decode(d, vv);
                v.set_int(vv);
            }
            break;
        }
    }
};

template<> struct codec_traits<cr::B> {
    static void encode(Encoder& e, const cr::B& v) {
        avro::encode(e, v.child);
    }
    static void decode(Decoder& d, cr::B& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.child);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.child);
        }
    }
};

template<> struct codec_traits<cr::A> {
    static void encode(Encoder& e, const cr::A& v) {
        avro::encode(e, v.edges);
    }
    static void decode(Decoder& d, cr::A& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.edges);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.edges);
        }
    }
};

}
#endif
