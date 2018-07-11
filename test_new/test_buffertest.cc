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
#include <catch.hpp>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/scoped_array.hpp>
#include <fstream>
#include <iostream>
#include "buffer/BufferStream.hh"
#include "buffer/BufferReader.hh"
#include "buffer/BufferPrint.hh"

using namespace avro;
using std::cout;
using std::endl;
using detail::kDefaultBlockSize;
using detail::kMinBlockSize;
using detail::kMaxBlockSize;

std::string makeString(size_t len) {
  std::string newstring;
  newstring.reserve(len);

  for (size_t i = 0; i < len; ++i) {
    char newchar = '0' + i % 16;
    if (newchar > '9') {
      newchar += 7;
    }
    newstring.push_back(newchar);
  }

  return newstring;
}

void printBuffer(const InputBuffer &buf) {
  avro::istream is(buf);
  cout << is.rdbuf() << endl;
}

TEST_CASE("Buffers: TestReserve", "[TestReserve]") {

  SECTION("Section 1") {
    OutputBuffer ob;
    REQUIRE(ob.size() == 0U);
    REQUIRE(ob.freeSpace() == 0U);
    REQUIRE(ob.numChunks() == 0);
    REQUIRE(ob.numDataChunks() == 0);
  }

  SECTION("Section 2") {
    size_t reserveSize = kMinBlockSize / 2;

    OutputBuffer ob(reserveSize);
    REQUIRE(ob.size() == 0U);
    REQUIRE(ob.freeSpace() == kMinBlockSize);
    REQUIRE(ob.numChunks() == 1);
    REQUIRE(ob.numDataChunks() == 0);

    // reserve should add a single block
    reserveSize += 8192;

    ob.reserve(reserveSize);
    REQUIRE(ob.size() == 0U);
    REQUIRE(ob.freeSpace() == reserveSize);
    REQUIRE(ob.numChunks() == 2);
    REQUIRE(ob.numDataChunks() == 0);

    // reserve should add two blocks, one of the maximum size and
    // one of the minimum size
    reserveSize += (kMaxBlockSize + kMinBlockSize / 2);

    ob.reserve(reserveSize);
    REQUIRE(ob.size() == 0U);
    REQUIRE(ob.freeSpace() == reserveSize + kMinBlockSize / 2);
    REQUIRE(ob.numChunks() == 4);
    REQUIRE(ob.numDataChunks() == 0);
  }
}

void addDataToBuffer(OutputBuffer &buf, size_t size) {
  std::string data = makeString(size);
  buf.writeTo(data.c_str(), data.size());
}

TEST_CASE("Buffers: TestGrow", "[TestGrow]") {

  OutputBuffer ob;

  // add exactly one block
  addDataToBuffer(ob, kDefaultBlockSize);

  REQUIRE(ob.size() == kDefaultBlockSize);
  REQUIRE(ob.freeSpace() == 0U);
  REQUIRE(ob.numChunks() == 0);
  REQUIRE(ob.numDataChunks() == 1);

  // add another block, half full
  addDataToBuffer(ob, kDefaultBlockSize / 2);

  REQUIRE(ob.size() == kDefaultBlockSize + kDefaultBlockSize / 2);
  REQUIRE(ob.freeSpace() == kDefaultBlockSize / 2);
  REQUIRE(ob.numChunks() == 1);
  REQUIRE(ob.numDataChunks() == 2);

  // reserve more capacity
  size_t reserveSize = ob.freeSpace() + 8192;
  ob.reserve(reserveSize);

  REQUIRE(ob.size() == kDefaultBlockSize + kDefaultBlockSize / 2);
  REQUIRE(ob.freeSpace() == reserveSize);
  REQUIRE(ob.numChunks() == 2);
  REQUIRE(ob.numDataChunks() == 2);

  // fill beyond capacity
  addDataToBuffer(ob, reserveSize + 1);
  REQUIRE(ob.size() == kDefaultBlockSize + kDefaultBlockSize / 2 + reserveSize + 1);
  REQUIRE(ob.freeSpace() == kDefaultBlockSize - 1);
  REQUIRE(ob.numChunks() == 1);
  REQUIRE(ob.numDataChunks() == 4);

}

TEST_CASE("Buffers: TestDiscard", "[TestDiscard]") {

  SECTION("Section 1") {
    OutputBuffer ob;
    size_t dataSize = kDefaultBlockSize * 2 + kDefaultBlockSize / 2;
    addDataToBuffer(ob, dataSize);

    REQUIRE(ob.size() == dataSize);
    REQUIRE(ob.freeSpace() == kDefaultBlockSize / 2);
    REQUIRE(ob.numChunks() == 1);
    REQUIRE(ob.numDataChunks() == 3);

    ob.discardData();

    REQUIRE(ob.size() == 0U);
    REQUIRE(ob.freeSpace() == kDefaultBlockSize / 2);
    REQUIRE(ob.numChunks() == 1);
    REQUIRE(ob.numDataChunks() == 0);
  }

  SECTION("Section 2") {
    // discard no bytes
    OutputBuffer ob;
    size_t dataSize = kDefaultBlockSize * 2 + kDefaultBlockSize / 2;
    addDataToBuffer(ob, dataSize);

    REQUIRE(ob.size() == dataSize);
    REQUIRE(ob.freeSpace() == kDefaultBlockSize / 2);
    REQUIRE(ob.numChunks() == 1);
    REQUIRE(ob.numDataChunks() == 3);

    ob.discardData(0);

    REQUIRE(ob.size() == dataSize);
    REQUIRE(ob.freeSpace() == kDefaultBlockSize / 2);
    REQUIRE(ob.numChunks() == 1);
    REQUIRE(ob.numDataChunks() == 3);
  }

  SECTION("Section 3") {
    // discard exactly one block
    OutputBuffer ob;
    size_t dataSize = kDefaultBlockSize * 2 + kDefaultBlockSize / 2;
    addDataToBuffer(ob, dataSize);

    REQUIRE(ob.size() == dataSize);
    REQUIRE(ob.freeSpace() == kDefaultBlockSize / 2);
    REQUIRE(ob.numChunks() == 1);
    REQUIRE(ob.numDataChunks() == 3);

    ob.discardData(kDefaultBlockSize);

    REQUIRE(ob.size() == dataSize - kDefaultBlockSize);
    REQUIRE(ob.freeSpace() == kDefaultBlockSize / 2);
    REQUIRE(ob.numChunks() == 1);
    REQUIRE(ob.numDataChunks() == 2);
  }

  SECTION("Section 4") {
    OutputBuffer ob;
    size_t dataSize = kDefaultBlockSize * 2 + kDefaultBlockSize / 2;
    addDataToBuffer(ob, dataSize);

    REQUIRE(ob.size() == dataSize);
    REQUIRE(ob.freeSpace() == kDefaultBlockSize / 2);
    REQUIRE(ob.numChunks() == 1);
    REQUIRE(ob.numDataChunks() == 3);

    size_t remainder = dataSize % 100;

    // discard data 100 bytes at a time
    size_t discarded = 0;
    while (ob.size() > 100) {
      ob.discardData(100);
      dataSize -= 100;
      discarded += 100;

      REQUIRE(ob.size() == dataSize);
      REQUIRE(ob.freeSpace() == kDefaultBlockSize / 2);
      REQUIRE(ob.numChunks() == 1);

      int chunks = 3 - (discarded / kDefaultBlockSize);
      REQUIRE(ob.numDataChunks() == chunks);
    }

    REQUIRE(ob.size() == remainder);
    REQUIRE(ob.freeSpace() == kDefaultBlockSize / 2);
    REQUIRE(ob.numChunks() == 1);
    REQUIRE(ob.numDataChunks() == 1);

    try {
      ob.discardData(ob.size() + 1);
    } catch (std::exception &e) {
      std::cout << "Intentionally triggered exception: " << e.what() << std::endl;
    }
    ob.discardData(ob.size());

    REQUIRE(ob.size() == 0U);
    REQUIRE(ob.freeSpace() == kDefaultBlockSize / 2);
    REQUIRE(ob.numChunks() == 1);
    REQUIRE(ob.numDataChunks() == 0);
  }
}

TEST_CASE("Buffers: TestConvertToInput", "[TestConvertToInput]") {

  OutputBuffer ob;
  size_t dataSize = kDefaultBlockSize * 2 + kDefaultBlockSize / 2;
  addDataToBuffer(ob, dataSize);

  InputBuffer ib(ob);

  REQUIRE(ib.size() == dataSize);
  REQUIRE(ib.numChunks() == 3);

  REQUIRE(ob.size() == dataSize);
  REQUIRE(ob.freeSpace() == kDefaultBlockSize / 2);
  REQUIRE(ob.numChunks() == 1);
  REQUIRE(ob.numDataChunks() == 3);

}

TEST_CASE("Buffers: TestExtractToInput", "[TestExtractToInput]") {

  SECTION("Section 1") {
    OutputBuffer ob;
    size_t dataSize = kDefaultBlockSize * 2 + kDefaultBlockSize / 2;
    addDataToBuffer(ob, dataSize);

    InputBuffer ib = ob.extractData();

    REQUIRE(ib.size() == dataSize);
    REQUIRE(ib.numChunks() == 3);

    REQUIRE(ob.size() == 0U);
    REQUIRE(ob.freeSpace() == kDefaultBlockSize / 2);
    REQUIRE(ob.numChunks() == 1);
    REQUIRE(ob.numDataChunks() == 0);
  }

  SECTION("Section 2") {
    // extract no bytes
    OutputBuffer ob;
    size_t dataSize = kDefaultBlockSize * 2 + kDefaultBlockSize / 2;
    addDataToBuffer(ob, dataSize);

    InputBuffer ib = ob.extractData(0);

    REQUIRE(ib.size() == 0U);
    REQUIRE(ib.numChunks() == 0);

    REQUIRE(ob.size() == dataSize);
    REQUIRE(ob.freeSpace() == kDefaultBlockSize / 2);
    REQUIRE(ob.numChunks() == 1);
    REQUIRE(ob.numDataChunks() == 3);
  }

  SECTION("Section 3") {
    // extract exactly one block
    OutputBuffer ob;
    size_t dataSize = kDefaultBlockSize * 2 + kDefaultBlockSize / 2;
    addDataToBuffer(ob, dataSize);

    InputBuffer ib = ob.extractData(kDefaultBlockSize);

    REQUIRE(ib.size() == kDefaultBlockSize);
    REQUIRE(ib.numChunks() == 1);

    REQUIRE(ob.size() == dataSize - kDefaultBlockSize);
    REQUIRE(ob.freeSpace() == kDefaultBlockSize / 2);
    REQUIRE(ob.numChunks() == 1);
    REQUIRE(ob.numDataChunks() == 2);
  }

  SECTION("Section 4") {
    OutputBuffer ob;
    size_t dataSize = kDefaultBlockSize * 2 + kDefaultBlockSize / 2;
    addDataToBuffer(ob, dataSize);

    size_t remainder = dataSize % 100;

    // extract data 100 bytes at a time
    size_t extracted = 0;
    while (ob.size() > 100) {
      ob.extractData(100);
      dataSize -= 100;
      extracted += 100;

      REQUIRE(ob.size() == dataSize);
      REQUIRE(ob.freeSpace() == kDefaultBlockSize / 2);
      REQUIRE(ob.numChunks() == 1);

      int chunks = 3 - (extracted / kDefaultBlockSize);
      REQUIRE(ob.numDataChunks() == chunks);
    }

    REQUIRE(ob.size() == remainder);
    REQUIRE(ob.freeSpace() == kDefaultBlockSize / 2);
    REQUIRE(ob.numChunks() == 1);
    REQUIRE(ob.numDataChunks() == 1);

    try {
      ob.extractData(ob.size() + 1);
    } catch (std::exception &e) {
      std::cout << "Intentionally triggered exception: " << e.what() << std::endl;
    }

    InputBuffer ib = ob.extractData(remainder);

    REQUIRE(ib.size() == remainder);
    REQUIRE(ib.numChunks() == 1);

    REQUIRE(ob.size() == 0U);
    REQUIRE(ob.freeSpace() == kDefaultBlockSize / 2);
    REQUIRE(ob.numChunks() == 1);
    REQUIRE(ob.numDataChunks() == 0);
  }
}

TEST_CASE("Buffers: TestAppend", "[TestAppend]") {

  OutputBuffer ob;
  size_t dataSize = kDefaultBlockSize + kDefaultBlockSize / 2;
  addDataToBuffer(ob, dataSize);

  OutputBuffer a;
  a.append(ob);

  REQUIRE(a.size() == dataSize);
  REQUIRE(a.freeSpace() == 0U);
  REQUIRE(a.numChunks() == 0);
  REQUIRE(a.numDataChunks() == 2);

  // reserve on a, then append from an input buffer
  a.reserve(7000);

  InputBuffer ib(ob);
  a.append(ib);

  REQUIRE(a.size() == dataSize * 2);
  REQUIRE(a.freeSpace() == 7000U);
  REQUIRE(a.numChunks() == 1);
  REQUIRE(a.numDataChunks() == 4);

}

TEST_CASE("Buffers: TestBufferStream", "[TestBufferStream]") {

  // write enough bytes to a buffer, to create at least 3 blocks
  std::string junk = makeString(kDefaultBlockSize);
  ostream os;
  int i = 0;
  for (; i < 3; ++i) {
    os << junk;
  }

  const OutputBuffer &buf = os.getBuffer();
  cout << "Buffer has " << buf.size() << " bytes\n";
  REQUIRE(buf.size() == junk.size() * i);

}

template<typename T>
void TestEof() {
  // create a message full of eof chars
  std::vector<char> eofs(sizeof (T) * 3 / 2, -1);

  OutputBuffer buf1;
  buf1.writeTo(&eofs[0], eofs.size());

  OutputBuffer buf2;
  buf2.writeTo(&eofs[0], eofs.size());

  // append the buffers, so the first 
  // character on a buffer boundary is eof
  buf1.append(buf2);

  avro::istream is(buf1);

  for (int i = 0; i < 3; ++i) {
    T d;
    char *addr = reinterpret_cast<char *> (&d);
    is.read(addr, sizeof (T));
    REQUIRE(is.gcount() == static_cast<std::streamsize> (sizeof (T)));
    REQUIRE(is.eof() == false);
  }

  char c;
  is.read(&c, sizeof (c));
  REQUIRE(is.gcount() == 0);
  REQUIRE(is.eof() == true);
}

TEST_CASE("Buffers: TestBufferStreamEof", "[TestBufferStreamEof]") {
  TestEof<int32_t>();
  TestEof<int64_t>();
  TestEof<float>();
  TestEof<double>();
}

TEST_CASE("Buffers: TestSeekAndTell", "[TestSeekAndTell]") {

  std::string junk = makeString(kDefaultBlockSize / 2);

  ostream os;

  // write enough bytes to a buffer, to create at least 3 blocks
  int i = 0;
  for (; i < 5; ++i) {
    os << junk;
  }

  const OutputBuffer &buf = os.getBuffer();
  cout << "Buffer has " << buf.size() << " bytes\n";

  istream is(os.getBuffer());
  REQUIRE(is.getBuffer().size() == junk.size() * i);
  is.seekg(2000);
  REQUIRE(is.tellg() == static_cast<std::streampos> (2000));
  is.seekg(6000);
  REQUIRE(is.tellg() == static_cast<std::streampos> (6000));
  is.seekg(is.getBuffer().size());
  REQUIRE(is.tellg() == static_cast<std::streampos> (is.getBuffer().size()));
  is.seekg(is.getBuffer().size() + 1);
  REQUIRE(is.tellg() == static_cast<std::streampos> (-1));


}

TEST_CASE("Buffers: TestReadSome", "[TestReadSome]") {

  std::string junk = makeString(kDefaultBlockSize / 2);

  ostream os;

  // write enough bytes to a buffer, to create at least 3 blocks
  int i = 0;
  for (; i < 5; ++i) {
    os << junk;
  }

  cout << "Buffer has " << os.getBuffer().size() << " bytes\n";

  istream is(os.getBuffer());

  char datain[5000];

  while (is.rdbuf()->in_avail()) {
    size_t bytesAvail = static_cast<size_t> (is.rdbuf()->in_avail());
    cout << "Bytes avail = " << bytesAvail << endl;
    size_t in = static_cast<size_t> (is.readsome(datain, sizeof (datain)));
    cout << "Bytes read = " << in << endl;
    REQUIRE(bytesAvail == in);
  }

}

TEST_CASE("Buffers: TestSeek", "[TestSeek]") {

  const std::string str = "SampleMessage";

  avro::OutputBuffer tmp1, tmp2, tmp3;
  tmp1.writeTo(str.c_str(), 3); // Sam
  tmp2.writeTo(str.c_str() + 3, 7); // pleMess
  tmp3.writeTo(str.c_str() + 10, 3); // age

  tmp2.append(tmp3);
  tmp1.append(tmp2);

  REQUIRE(tmp3.numDataChunks() == 1);
  REQUIRE(tmp2.numDataChunks() == 2);
  REQUIRE(tmp1.numDataChunks() == 3);

  avro::InputBuffer buf(tmp1);

  cout << "Starting string: " << str << '\n';
  REQUIRE(static_cast<std::string::size_type> (buf.size()) == str.size());

  avro::istream is(buf);

  const std::string part1 = "Sample";
  char buffer[16];
  is.read(buffer, part1.size());
  std::string sample1(buffer, part1.size());
  cout << "After reading bytes: " << sample1 << '\n';
  REQUIRE(sample1 == part1);

  const std::string part2 = "Message";
  is.read(buffer, part2.size());
  std::string sample2(buffer, part2.size());
  cout << "After reading remaining bytes: " << sample2 << '\n';
  REQUIRE(sample2 == part2);

  cout << "Seeking back " << '\n';
  is.seekg(-static_cast<std::streamoff> (part2.size()), std::ios_base::cur);

  std::streampos loc = is.tellg();
  cout << "Saved loc = " << loc << '\n';
  REQUIRE(static_cast<std::string::size_type> (loc) == (str.size() - part2.size()));

  cout << "Reading remaining bytes: " << is.rdbuf() << '\n';
  cout << "bytes avail = " << is.rdbuf()->in_avail() << '\n';
  REQUIRE(is.rdbuf()->in_avail() == 0);

  cout << "Moving to saved loc = " << loc << '\n';
  is.seekg(loc);
  cout << "bytes avail = " << is.rdbuf()->in_avail() << '\n';

  std::ostringstream oss;
  oss << is.rdbuf();
  cout << "After reading bytes: " << oss.str() << '\n';
  REQUIRE(oss.str() == part2);


}

TEST_CASE("Buffers: TestIterator", "[TestIterator]") {

  OutputBuffer ob(2 * kMaxBlockSize + 10);
  REQUIRE(ob.numChunks() == 3);
  REQUIRE(ob.size() == 0U);
  REQUIRE(ob.freeSpace() == 2 * kMaxBlockSize + kMinBlockSize);

  REQUIRE(std::distance(ob.begin(), ob.end()) == 3);

  OutputBuffer::const_iterator iter = ob.begin();
  REQUIRE(iter->size() == kMaxBlockSize);
  ++iter;
  REQUIRE(iter->size() == kMaxBlockSize);
  ++iter;
  REQUIRE(iter->size() == kMinBlockSize);
  ++iter;
  REQUIRE(iter == ob.end());

  size_t toWrite = kMaxBlockSize + kMinBlockSize;
  ob.wroteTo(toWrite);
  REQUIRE(ob.size() == toWrite);
  REQUIRE(ob.freeSpace() == kMaxBlockSize);
  REQUIRE(ob.numChunks() == 2);
  REQUIRE(ob.numDataChunks() == 2);

  InputBuffer ib = ob;
  REQUIRE(std::distance(ib.begin(), ib.end()) == 2);

  size_t acc = 0;
  for (OutputBuffer::const_iterator iter = ob.begin();
    iter != ob.end();
    ++iter) {
    acc += iter->size();
  }
  REQUIRE(ob.freeSpace() == acc);

  try {
    ob.wroteTo(acc + 1);
  } catch (std::exception &e) {
    std::cout << "Intentionally triggered exception: " << e.what() << std::endl;
  }

}

TEST_CASE("Buffers: TestSplit", "[TestSplit]") {

  const std::string str = "This message is to be split";

  avro::OutputBuffer buf;
  buf.writeTo(str.c_str(), str.size());

  char datain[12];
  avro::istream is(buf);
  size_t in = static_cast<size_t> (is.readsome(datain, sizeof (datain)));
  REQUIRE(in == sizeof (datain));
  REQUIRE(static_cast<size_t> (is.tellg()) == sizeof (datain));

  OutputBuffer part2;
  part2.append(is.getBuffer());
  REQUIRE(part2.size() == buf.size());
  InputBuffer part1 = part2.extractData(static_cast<size_t> (is.tellg()));

  REQUIRE(part2.size() == str.size() - in);

  printBuffer(part1);
  printBuffer(part2);

}

TEST_CASE("Buffers: TestSplitOnBorder", "[TestSplitOnBorder]") {

  const std::string part1 = "This message";
  const std::string part2 = " is to be split";

  avro::OutputBuffer buf;
  buf.writeTo(part1.c_str(), part1.size());
  size_t firstChunkSize = buf.size();

  {
    avro::OutputBuffer tmp;
    tmp.writeTo(part2.c_str(), part2.size());
    buf.append(tmp);
    printBuffer(InputBuffer(buf));
  }

  REQUIRE(buf.numDataChunks() == 2);
  size_t bufsize = buf.size();

  boost::scoped_array<char> datain(new char[firstChunkSize]);
  avro::istream is(buf);
  size_t in = static_cast<size_t> (is.readsome(&datain[0], firstChunkSize));
  REQUIRE(in == firstChunkSize);

  OutputBuffer newBuf;
  newBuf.append(is.getBuffer());
  newBuf.discardData(static_cast<size_t> (is.tellg()));
  REQUIRE(newBuf.numDataChunks() == 1);

  REQUIRE(newBuf.size() == bufsize - in);

  cout << is.rdbuf() << endl;
  printBuffer(newBuf);

}

TEST_CASE("Buffers: TestSplitTwice", "[TestSplitTwice]") {

  const std::string msg1 = makeString(30);

  avro::OutputBuffer buf1;
  buf1.writeTo(msg1.c_str(), msg1.size());

  REQUIRE(buf1.size() == msg1.size());

  printBuffer(buf1);

  avro::istream is(buf1);
  char buffer[6];
  is.readsome(buffer, 5);
  buffer[5] = 0;
  std::cout << "buffer =" << buffer << std::endl;

  buf1.discardData(static_cast<size_t> (is.tellg()));
  printBuffer(buf1);

  avro::istream is2(buf1);
  is2.seekg(15);

  buf1.discardData(static_cast<size_t> (is2.tellg()));
  printBuffer(buf1);

}

TEST_CASE("Buffers: TestCopy", "[TestCopy]") {

  const std::string msg = makeString(30);
  // Test1, small data, small buffer

  SECTION("Section 1") {
    std::cout << "Test1\n";
    // put a small amount of data in the buffer
    avro::OutputBuffer wb;

    wb.writeTo(msg.c_str(), msg.size());

    REQUIRE(msg.size() == wb.size());
    REQUIRE(wb.numDataChunks() == 1);
    REQUIRE((kDefaultBlockSize - msg.size()) == wb.freeSpace());

    // copy starting at offset 5 and copying 10 less bytes
    BufferReader br(wb);
    br.seek(5);
    avro::InputBuffer ib = br.copyData(msg.size() - 10);

    printBuffer(ib);

    REQUIRE(ib.numChunks() == 1);
    REQUIRE(ib.size() == msg.size() - 10);

    // buf 1 should be unchanged
    REQUIRE(msg.size() == wb.size());
    REQUIRE(wb.numDataChunks() == 1);
    REQUIRE((kDefaultBlockSize - msg.size()) == wb.freeSpace());

    // make sure wb is still functional
    wb.reserve(kDefaultBlockSize);
    REQUIRE(wb.size() == msg.size());
    REQUIRE(wb.numChunks() == 2);
    REQUIRE((kDefaultBlockSize * 2 - msg.size()) == wb.freeSpace());
  }

  // Test2, small data, large buffer

  SECTION("Section 2") {
    std::cout << "Test2\n";
    // put a small amount of data in the buffer
    const OutputBuffer::size_type bufsize = 3 * kMaxBlockSize;

    avro::OutputBuffer wb(bufsize);
    REQUIRE(wb.numChunks() == 3);
    REQUIRE(wb.freeSpace() == bufsize);

    wb.writeTo(msg.c_str(), msg.size());

    REQUIRE(wb.size() == msg.size());
    REQUIRE(wb.numDataChunks() == 1);
    REQUIRE((bufsize - msg.size()) == wb.freeSpace());

    BufferReader br(wb);
    br.seek(5);
    avro::InputBuffer ib = br.copyData(msg.size() - 10);

    printBuffer(ib);

    REQUIRE(ib.numChunks() == 1);
    REQUIRE(ib.size() == msg.size() - 10);

    // wb should be unchanged
    REQUIRE(msg.size() == wb.size());
    REQUIRE(wb.numChunks() == 3);
    REQUIRE(wb.numDataChunks() == 1);
    REQUIRE(bufsize - msg.size() == wb.freeSpace());

    // reserving a small amount should have no effect
    wb.reserve(1);
    REQUIRE(msg.size() == wb.size());
    REQUIRE(wb.numChunks() == 3);
    REQUIRE(bufsize - msg.size() == wb.freeSpace());

    // reserve more (will get extra block)
    wb.reserve(bufsize);
    REQUIRE(msg.size() == wb.size());
    REQUIRE(wb.numChunks() == 4);
    REQUIRE((kMaxBlockSize * 3 - msg.size() + kMinBlockSize) == wb.freeSpace());
  }

  // Test3 Border case, buffer is exactly full

  SECTION("Section 3") {
    std::cout << "Test3\n";
    const OutputBuffer::size_type bufsize = 2 * kDefaultBlockSize;
    avro::OutputBuffer wb;

    for (unsigned i = 0; i < bufsize; ++i) {
      wb.writeTo('a');
    }

    REQUIRE(wb.size() == bufsize);
    REQUIRE(wb.freeSpace() == 0U);
    REQUIRE(wb.numChunks() == 0);
    REQUIRE(wb.numDataChunks() == 2);

    // copy where the chunks overlap
    BufferReader br(wb);
    br.seek(bufsize / 2 - 10);
    avro::InputBuffer ib = br.copyData(20);

    printBuffer(ib);

    REQUIRE(ib.size() == 20U);
    REQUIRE(ib.numChunks() == 2);

    // wb should be unchanged
    REQUIRE(wb.size() == bufsize);
    REQUIRE(wb.freeSpace() == 0U);
    REQUIRE(wb.numDataChunks() == 2);
  }

  // Test4, no data 

  SECTION("Section 4") {
    const OutputBuffer::size_type bufsize = 2 * kMaxBlockSize;
    std::cout << "Test4\n";
    avro::OutputBuffer wb(bufsize);
    REQUIRE(wb.numChunks() == 2);
    REQUIRE(wb.size() == 0U);
    REQUIRE(wb.freeSpace() == bufsize);

    avro::InputBuffer ib;
    try {
      BufferReader br(wb);
      br.seek(10);
    } catch (std::exception &e) {
      cout << "Intentially triggered exception: " << e.what() << endl;
    }
    try {
      BufferReader br(wb);
      avro::InputBuffer ib = br.copyData(10);
    } catch (std::exception &e) {
      cout << "Intentially triggered exception: " << e.what() << endl;
    }


    REQUIRE(ib.numChunks() == 0);
    REQUIRE(ib.size() == 0U);

    // wb should keep all blocks remaining
    REQUIRE(wb.numChunks() == 2);
    REQUIRE(wb.size() == 0U);
    REQUIRE(wb.freeSpace() == bufsize);
  }
}

// this is reproducing a sequence of steps that caused a crash

TEST_CASE("Buffers: TestBug", "[TestBug]") {

  OutputBuffer rxBuf;
  OutputBuffer buf;
  rxBuf.reserve(64 * 1024);

  rxBuf.wroteTo(2896);

  {
    avro::InputBuffer ib(rxBuf.extractData());
    buf.append(ib);
  }

  buf.discardData(61);

  rxBuf.reserve(64 * 1024);
  rxBuf.wroteTo(381);

  {
    avro::InputBuffer ib(rxBuf.extractData());
    buf.append(ib);
  }

  buf.discardData(3216);


  rxBuf.reserve(64 * 1024);

}

bool safeToDelete = false;

void deleteForeign(const std::string &val) {
  std::cout << "Deleting foreign string containing " << val << '\n';
  REQUIRE(safeToDelete);
}

TEST_CASE("Buffers: TestForeign", "[TestForeign]") {
  {
    std::string hello = "hello ";
    std::string there = "there ";
    std::string world = "world ";

    OutputBuffer copy;

    {
      OutputBuffer buf;
      buf.writeTo(hello.c_str(), hello.size());
      buf.appendForeignData(there.c_str(), there.size(), boost::bind(&deleteForeign, there));
      buf.writeTo(world.c_str(), world.size());

      printBuffer(buf);
      REQUIRE(buf.size() == 18U);
      copy = buf;
    }
    std::cout << "Leaving inner scope\n";
    safeToDelete = true;
  }
  std::cout << "Leaving outer scope\n";
  safeToDelete = false;
}

TEST_CASE("Buffers: TestForeignDiscard", "[TestForeignDiscard]") {
  std::string hello = "hello ";
  std::string again = "again ";
  std::string there = "there ";
  std::string world = "world ";

  OutputBuffer buf;
  buf.writeTo(hello.c_str(), hello.size());
  buf.appendForeignData(again.c_str(), again.size(), boost::bind(&deleteForeign, again));
  buf.appendForeignData(there.c_str(), there.size(), boost::bind(&deleteForeign, there));
  buf.writeTo(world.c_str(), world.size());

  printBuffer(buf);
  REQUIRE(buf.size() == 24U);

  // discard some data including half the foreign buffer
  buf.discardData(9);
  printBuffer(buf);
  REQUIRE(buf.size() == 15U);

  // discard some more data, which will lop off the first foreign buffer
  safeToDelete = true;
  buf.discardData(6);
  safeToDelete = false;
  printBuffer(buf);
  REQUIRE(buf.size() == 9U);

  // discard some more data, which will lop off the second foreign buffer
  safeToDelete = true;
  buf.discardData(3);
  safeToDelete = false;
  printBuffer(buf);
  REQUIRE(buf.size() == 6U);
}

TEST_CASE("Buffers: TestPrinter", "[TestPrinter]") {
  OutputBuffer ob;
  addDataToBuffer(ob, 128);
  std::cout << ob << std::endl;
}
