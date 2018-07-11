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


#ifndef PRIMITIVETYPES_HH_2542349456__H_
#define PRIMITIVETYPES_HH_2542349456__H_


#include <sstream>
#include "boost/any.hpp"
#include "Specific.hh"
#include "Encoder.hh"
#include "Decoder.hh"

namespace pt {
struct TestPrimitiveTypes {
    avro::null Null;
    bool Boolean;
    int32_t Int;
    int64_t Long;
    float Float;
    double Double;
    std::vector<uint8_t> Bytes;
    std::string String;
    avro::null SecondNull;
    TestPrimitiveTypes() :
        Null(avro::null()),
        Boolean(bool()),
        Int(int32_t()),
        Long(int64_t()),
        Float(float()),
        Double(double()),
        Bytes(std::vector<uint8_t>()),
        String(std::string()),
        SecondNull(avro::null())
        { }
};

}
namespace avro {
template<> struct codec_traits<pt::TestPrimitiveTypes> {
    static void encode(Encoder& e, const pt::TestPrimitiveTypes& v) {
        avro::encode(e, v.Null);
        avro::encode(e, v.Boolean);
        avro::encode(e, v.Int);
        avro::encode(e, v.Long);
        avro::encode(e, v.Float);
        avro::encode(e, v.Double);
        avro::encode(e, v.Bytes);
        avro::encode(e, v.String);
        avro::encode(e, v.SecondNull);
    }
    static void decode(Decoder& d, pt::TestPrimitiveTypes& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.Null);
                    break;
                case 1:
                    avro::decode(d, v.Boolean);
                    break;
                case 2:
                    avro::decode(d, v.Int);
                    break;
                case 3:
                    avro::decode(d, v.Long);
                    break;
                case 4:
                    avro::decode(d, v.Float);
                    break;
                case 5:
                    avro::decode(d, v.Double);
                    break;
                case 6:
                    avro::decode(d, v.Bytes);
                    break;
                case 7:
                    avro::decode(d, v.String);
                    break;
                case 8:
                    avro::decode(d, v.SecondNull);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.Null);
            avro::decode(d, v.Boolean);
            avro::decode(d, v.Int);
            avro::decode(d, v.Long);
            avro::decode(d, v.Float);
            avro::decode(d, v.Double);
            avro::decode(d, v.Bytes);
            avro::decode(d, v.String);
            avro::decode(d, v.SecondNull);
        }
    }
};

}
#endif
