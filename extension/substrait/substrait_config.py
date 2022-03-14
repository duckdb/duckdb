import os

# list all include directories
include_directories = [
    os.path.sep.join(x.split("/"))
    for x in [
        "extension/substrait/include/",
        "third_party/",
        "third_party/substrait/",
        "third_party/google/"
    ]
]
# source files
source_files = [
    os.path.sep.join(x.split("/"))
    for x in [
        "extension/substrait/substrait-extension.cpp",
        "extension/substrait/from_substrait.cpp",
        "extension/substrait/to_substrait.cpp",
        # Substrait .cc files
        "third_party/substrait/substrait/algebra.pb.cc",
        "third_party/substrait/substrait/capabilities.pb.cc",
        "third_party/substrait/substrait/function.pb.cc",
        "third_party/substrait/substrait/parameterized_types.pb.cc",
        "third_party/substrait/substrait/plan.pb.cc",
        "third_party/substrait/substrait/type.pb.cc",
        "third_party/substrait/substrait/type_expressions.pb.cc",
        "third_party/substrait/substrait/extensions/extensions.pb.cc",
        # Protobuf common
        "third_party/google/protobuf/any.cc",
        "third_party/google/protobuf/any.pb.cc",
        "third_party/google/protobuf/any_lite.cc",
        "third_party/google/protobuf/arena.cc",
        "third_party/google/protobuf/arenastring.cc",
        "third_party/google/protobuf/descriptor.cc",
        "third_party/google/protobuf/descriptor.pb.cc",
        "third_party/google/protobuf/descriptor_database.cc",
        "third_party/google/protobuf/dynamic_message.cc",
        "third_party/google/protobuf/extension_set.cc",
        "third_party/google/protobuf/extension_set_heavy.cc",
        "third_party/google/protobuf/generated_enum_util.cc",
        "third_party/google/protobuf/generated_message_bases.cc",
        "third_party/google/protobuf/generated_message_reflection.cc",
        "third_party/google/protobuf/generated_message_table_driven.cc",
        "third_party/google/protobuf/generated_message_table_driven_lite.cc",
        "third_party/google/protobuf/generated_message_util.cc",
        "third_party/google/protobuf/implicit_weak_message.cc",
        "third_party/google/protobuf/inlined_string_field.cc",
        "third_party/google/protobuf/map.cc",
        "third_party/google/protobuf/map_field.cc",
        "third_party/google/protobuf/message.cc",
        "third_party/google/protobuf/message_lite.cc",
        "third_party/google/protobuf/parse_context.cc",
        "third_party/google/protobuf/reflection_ops.cc",
        "third_party/google/protobuf/repeated_field.cc",
        "third_party/google/protobuf/repeated_ptr_field.cc",
        "third_party/google/protobuf/text_format.cc",
        "third_party/google/protobuf/unknown_field_set.cc",
        "third_party/google/protobuf/wire_format.cc",
        "third_party/google/protobuf/wire_format_lite.cc",
        # Protobuf io
        "third_party/google/protobuf/io/coded_stream.cc",
        "third_party/google/protobuf/io/io_win32.cc",
        "third_party/google/protobuf/io/strtod.cc",
        "third_party/google/protobuf/io/tokenizer.cc",
        "third_party/google/protobuf/io/zero_copy_stream.cc",
        "third_party/google/protobuf/io/zero_copy_stream_impl.cc",
        "third_party/google/protobuf/io/zero_copy_stream_impl_lite.cc",
        # Protobuf stubs
        "third_party/google/protobuf/stubs/common.cc",
        "third_party/google/protobuf/stubs/int128.cc",
        "third_party/google/protobuf/stubs/status.cc",
        "third_party/google/protobuf/stubs/stringpiece.cc",
        "third_party/google/protobuf/stubs/stringprintf.cc",
        "third_party/google/protobuf/stubs/structurally_valid.cc",
        "third_party/google/protobuf/stubs/strutil.cc",
        "third_party/google/protobuf/stubs/substitute.cc",
    ]
]
