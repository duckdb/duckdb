cmake_minimum_required(VERSION 2.8.12)

set(CMAKE_CXX_STANDARD 11)
project(SubstraitExtension)

include_directories(include ../../third_party/substrait)
include_directories(include ../../third_party/)

set(PROTOBUF_SOURCES
    ../../third_party/google/protobuf/any.cc
    ../../third_party/google/protobuf/repeated_field.h
    ../../third_party/google/protobuf/descriptor.pb.cc
    ../../third_party/google/protobuf/generated_message_table_driven_lite.cc
    ../../third_party/google/protobuf/any_lite.cc
    ../../third_party/google/protobuf/descriptor_database.cc
    ../../third_party/google/protobuf/port_undef.inc
    ../../third_party/google/protobuf/reflection_ops.cc
    ../../third_party/google/protobuf/repeated_ptr_field.h
    ../../third_party/google/protobuf/port.h
    ../../third_party/google/protobuf/arena.cc
    ../../third_party/google/protobuf/inlined_string_field.cc
    ../../third_party/google/protobuf/wire_format.h
    ../../third_party/google/protobuf/explicitly_constructed.h
    ../../third_party/google/protobuf/inlined_string_field.h
    ../../third_party/google/protobuf/LICENSE
    ../../third_party/google/protobuf/map.h
    ../../third_party/google/protobuf/arenastring.h
    ../../third_party/google/protobuf/generated_message_tctable_decl.h
    ../../third_party/google/protobuf/map_entry_lite.h
    ../../third_party/google/protobuf/extension_set_heavy.cc
    ../../third_party/google/protobuf/map.cc
    ../../third_party/google/protobuf/descriptor.h
    ../../third_party/google/protobuf/message_lite.cc
    ../../third_party/google/protobuf/wire_format_lite.cc
    ../../third_party/google/protobuf/parse_context.cc
    ../../third_party/google/protobuf/message.h
    ../../third_party/google/protobuf/unknown_field_set.cc
    ../../third_party/google/protobuf/text_format.h
    ../../third_party/google/protobuf/any.pb.cc
    ../../third_party/google/protobuf/has_bits.h
    ../../third_party/google/protobuf/arena_impl.h
    ../../third_party/google/protobuf/repeated_field.cc
    ../../third_party/google/protobuf/extension_set.h
    ../../third_party/google/protobuf/reflection_ops.h
    ../../third_party/google/protobuf/generated_message_reflection.cc
    ../../third_party/google/protobuf/implicit_weak_message.cc
    ../../third_party/google/protobuf/generated_message_table_driven.h
    ../../third_party/google/protobuf/reflection_internal.h
    ../../third_party/google/protobuf/generated_message_reflection.h
    ../../third_party/google/protobuf/unknown_field_set.h
    ../../third_party/google/protobuf/map_field_inl.h
    ../../third_party/google/protobuf/generated_message_tctable_impl.inc
    ../../third_party/google/protobuf/descriptor.pb.h
    ../../third_party/google/protobuf/arenastring.cc
    ../../third_party/google/protobuf/generated_message_bases.h
    ../../third_party/google/protobuf/map_entry.h
    ../../third_party/google/protobuf/message_lite.h
    ../../third_party/google/protobuf/generated_message_util.h
    ../../third_party/google/protobuf/descriptor_database.h
    ../../third_party/google/protobuf/map_field.h
    ../../third_party/google/protobuf/generated_message_tctable_impl.h
    ../../third_party/google/protobuf/generated_message_table_driven_lite.h
    ../../third_party/google/protobuf/generated_message_util.cc
    ../../third_party/google/protobuf/generated_message_bases.cc
    ../../third_party/google/protobuf/generated_enum_reflection.h
    ../../third_party/google/protobuf/generated_enum_util.h
    ../../third_party/google/protobuf/any.pb.h
    ../../third_party/google/protobuf/message.cc
    ../../third_party/google/protobuf/map_type_handler.h
    ../../third_party/google/protobuf/parse_context.h
    ../../third_party/google/protobuf/extension_set_inl.h
    ../../third_party/google/protobuf/map_field_lite.h
    ../../third_party/google/protobuf/descriptor.cc
    ../../third_party/google/protobuf/metadata_lite.h
    ../../third_party/google/protobuf/wire_format.cc
    ../../third_party/google/protobuf/dynamic_message.cc
    ../../third_party/google/protobuf/implicit_weak_message.h
    ../../third_party/google/protobuf/arena.h
    ../../third_party/google/protobuf/map_field.cc
    ../../third_party/google/protobuf/any.h
    ../../third_party/google/protobuf/repeated_ptr_field.cc
    ../../third_party/google/protobuf/reflection.h
    ../../third_party/google/protobuf/dynamic_message.h
    ../../third_party/google/protobuf/text_format.cc
    ../../third_party/google/protobuf/port_def.inc
    ../../third_party/google/protobuf/extension_set.cc
    ../../third_party/google/protobuf/generated_enum_util.cc
    ../../third_party/google/protobuf/generated_message_table_driven.cc
    ../../third_party/google/protobuf/wire_format_lite.h
    ../../third_party/google/protobuf/io/zero_copy_stream_impl.h
    ../../third_party/google/protobuf/io/zero_copy_stream_impl_lite.cc
    ../../third_party/google/protobuf/io/zero_copy_stream_impl.cc
    ../../third_party/google/protobuf/io/coded_stream.cc
    ../../third_party/google/protobuf/io/zero_copy_stream_impl_lite.h
    ../../third_party/google/protobuf/io/strtod.h
    ../../third_party/google/protobuf/io/tokenizer.cc
    ../../third_party/google/protobuf/io/zero_copy_stream.cc
    ../../third_party/google/protobuf/io/tokenizer.h
    ../../third_party/google/protobuf/io/strtod.cc
    ../../third_party/google/protobuf/io/zero_copy_stream.h
    ../../third_party/google/protobuf/io/io_win32.cc
    ../../third_party/google/protobuf/io/io_win32.h
    ../../third_party/google/protobuf/io/coded_stream.h
    ../../third_party/google/protobuf/stubs/int128.cc
    ../../third_party/google/protobuf/stubs/structurally_valid.cc
    ../../third_party/google/protobuf/stubs/stringpiece.h
    ../../third_party/google/protobuf/stubs/stl_util.h
    ../../third_party/google/protobuf/stubs/status.cc
    ../../third_party/google/protobuf/stubs/port.h
    ../../third_party/google/protobuf/stubs/status.h
    ../../third_party/google/protobuf/stubs/common.cc
    ../../third_party/google/protobuf/stubs/map_util.h
    ../../third_party/google/protobuf/stubs/casts.h
    ../../third_party/google/protobuf/stubs/stringprintf.h
    ../../third_party/google/protobuf/stubs/logging.h
    ../../third_party/google/protobuf/stubs/common.h
    ../../third_party/google/protobuf/stubs/macros.h
    ../../third_party/google/protobuf/stubs/platform_macros.h
    ../../third_party/google/protobuf/stubs/once.h
    ../../third_party/google/protobuf/stubs/strutil.cc
    ../../third_party/google/protobuf/stubs/int128.h
    ../../third_party/google/protobuf/stubs/stringprintf.cc
    ../../third_party/google/protobuf/stubs/mutex.h
    ../../third_party/google/protobuf/stubs/substitute.cc
    ../../third_party/google/protobuf/stubs/substitute.h
    ../../third_party/google/protobuf/stubs/callback.h
    ../../third_party/google/protobuf/stubs/strutil.h
    ../../third_party/google/protobuf/stubs/hash.h
    ../../third_party/google/protobuf/stubs/stringpiece.cc)

set(SUBSTRAIT_SOURCES
    ../../third_party/substrait/substrait/capabilities.pb.cc
    ../../third_party/substrait/substrait/expression.pb.cc
    ../../third_party/substrait/substrait/function.pb.cc
    ../../third_party/substrait/substrait/parameterized_types.pb.cc
    ../../third_party/substrait/substrait/plan.pb.cc
    ../../third_party/substrait/substrait/relations.pb.cc
    ../../third_party/substrait/substrait/type.pb.cc
    ../../third_party/substrait/substrait/type_expressions.pb.cc
    ../../third_party/substrait/substrait/extensions/extensions.pb.cc
    ../../third_party/substrait/substrait/capabilities.pb.h
    ../../third_party/substrait/substrait/expression.pb.h
    ../../third_party/substrait/substrait/function.pb.h
    ../../third_party/substrait/substrait/parameterized_types.pb.h
    ../../third_party/substrait/substrait/plan.pb.h
    ../../third_party/substrait/substrait/relations.pb.h
    ../../third_party/substrait/substrait/type.pb.h
    ../../third_party/substrait/substrait/type_expressions.pb.h
    ../../third_party/substrait/substrait/extensions/extensions.pb.h)

set(ALL_SOURCES to_substrait.cpp from_substrait.cpp substrait-extension.cpp
                ${SUBSTRAIT_SOURCES} ${PROTOBUF_SOURCES})

add_library(substrait_extension STATIC ${ALL_SOURCES})

build_loadable_extension(substrait ${ALL_SOURCES})

install(
  TARGETS substrait_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
