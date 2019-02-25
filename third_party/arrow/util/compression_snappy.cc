// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/util/compression_snappy.h"

#include <cstddef>
#include <cstdint>
#include <sstream>

#include <snappy.h>

#include "arrow/status.h"
#include "arrow/util/macros.h"

using std::size_t;

namespace arrow {
namespace util {

// ----------------------------------------------------------------------
// Snappy implementation

Status SnappyCodec::MakeCompressor(std::shared_ptr<Compressor>* out) {
  return Status::NotImplemented("Streaming compression unsupported with Snappy");
}

Status SnappyCodec::MakeDecompressor(std::shared_ptr<Decompressor>* out) {
  return Status::NotImplemented("Streaming decompression unsupported with Snappy");
}

Status SnappyCodec::Decompress(int64_t input_len, const uint8_t* input,
                               int64_t output_buffer_len, uint8_t* output_buffer) {
  return Decompress(input_len, input, output_buffer_len, output_buffer, nullptr);
}

Status SnappyCodec::Decompress(int64_t input_len, const uint8_t* input,
                               int64_t output_buffer_len, uint8_t* output_buffer,
                               int64_t* output_len) {
  size_t decompressed_size;
  if (!snappy::GetUncompressedLength(reinterpret_cast<const char*>(input),
                                     static_cast<size_t>(input_len),
                                     &decompressed_size)) {
    return Status::IOError("Corrupt snappy compressed data.");
  }
  if (output_buffer_len < static_cast<int64_t>(decompressed_size)) {
    return Status::Invalid("Output buffer size (", output_buffer_len, ") must be ",
                           decompressed_size, " or larger.");
  }
  if (output_len) {
    *output_len = static_cast<int64_t>(decompressed_size);
  }
  if (!snappy::RawUncompress(reinterpret_cast<const char*>(input),
                             static_cast<size_t>(input_len),
                             reinterpret_cast<char*>(output_buffer))) {
    return Status::IOError("Corrupt snappy compressed data.");
  }
  return Status::OK();
}

int64_t SnappyCodec::MaxCompressedLen(int64_t input_len,
                                      const uint8_t* ARROW_ARG_UNUSED(input)) {
  return snappy::MaxCompressedLength(input_len);
}

Status SnappyCodec::Compress(int64_t input_len, const uint8_t* input,
                             int64_t ARROW_ARG_UNUSED(output_buffer_len),
                             uint8_t* output_buffer, int64_t* output_len) {
  size_t output_size;
  snappy::RawCompress(reinterpret_cast<const char*>(input),
                      static_cast<size_t>(input_len),
                      reinterpret_cast<char*>(output_buffer), &output_size);
  *output_len = static_cast<int64_t>(output_size);
  return Status::OK();
}

}  // namespace util
}  // namespace arrow
