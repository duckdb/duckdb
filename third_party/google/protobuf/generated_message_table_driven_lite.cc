// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
// https://developers.google.com/protocol-buffers/
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#include <google/protobuf/generated_message_table_driven_lite.h>

#include <type_traits>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/metadata_lite.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/wire_format_lite.h>

namespace google {
namespace protobuf {
namespace internal {

namespace {

std::string* MutableUnknownFields(MessageLite* msg, int64_t arena_offset) {
  return Raw<InternalMetadata>(msg, arena_offset)
      ->mutable_unknown_fields<std::string>();
}

struct UnknownFieldHandlerLite {
  // TODO(mvels): consider renaming UnknownFieldHandler to (TableDrivenTraits?),
  // and conflating InternalMetaData into it, simplifying the template.
  static constexpr bool IsLite() { return true; }

  static bool Skip(MessageLite* msg, const ParseTable& table,
                   io::CodedInputStream* input, int tag) {
    GOOGLE_DCHECK(!table.unknown_field_set);
    io::StringOutputStream unknown_fields_string(
        MutableUnknownFields(msg, table.arena_offset));
    io::CodedOutputStream unknown_fields_stream(&unknown_fields_string, false);

    return internal::WireFormatLite::SkipField(input, tag,
                                               &unknown_fields_stream);
  }

  static void Varint(MessageLite* msg, const ParseTable& table, int tag,
                     int value) {
    GOOGLE_DCHECK(!table.unknown_field_set);

    io::StringOutputStream unknown_fields_string(
        MutableUnknownFields(msg, table.arena_offset));
    io::CodedOutputStream unknown_fields_stream(&unknown_fields_string, false);
    unknown_fields_stream.WriteVarint32(tag);
    unknown_fields_stream.WriteVarint32(value);
  }

  static bool ParseExtension(MessageLite* msg, const ParseTable& table,
                             io::CodedInputStream* input, int tag) {
    ExtensionSet* extensions = GetExtensionSet(msg, table.extension_offset);
    if (extensions == nullptr) {
      return false;
    }

    const MessageLite* prototype = table.default_instance();

    GOOGLE_DCHECK(!table.unknown_field_set);
    io::StringOutputStream unknown_fields_string(
        MutableUnknownFields(msg, table.arena_offset));
    io::CodedOutputStream unknown_fields_stream(&unknown_fields_string, false);
    return extensions->ParseField(tag, input, prototype,
                                  &unknown_fields_stream);
  }
};

}  // namespace

bool MergePartialFromCodedStreamLite(MessageLite* msg, const ParseTable& table,
                                     io::CodedInputStream* input) {
  return MergePartialFromCodedStreamImpl<UnknownFieldHandlerLite>(msg, table,
                                                                  input);
}

}  // namespace internal
}  // namespace protobuf
}  // namespace google
