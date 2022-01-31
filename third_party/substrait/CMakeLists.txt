# FIXME Inline protobuf dependency?
find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

include_directories(./)

set(SUBSTRAIT_SOURCES
    substrait/capabilities.pb.cc
    substrait/expression.pb.cc
    substrait/function.pb.cc
    substrait/parameterized_types.pb.cc
    substrait/plan.pb.cc
    substrait/relations.pb.cc
    substrait/type.pb.cc
    substrait/type_expressions.pb.cc
    substrait/extensions/extensions.pb.cc)

set(SUBSTRAIT_HEADERS
    substrait/capabilities.pb.h
    substrait/expression.pb.h
    substrait/function.pb.h
    substrait/parameterized_types.pb.h
    substrait/plan.pb.h
    substrait/relations.pb.h
    substrait/type.pb.h
    substrait/type_expressions.pb.h
    substrait/extensions/extensions.pb.h)

add_library(substrait_og STATIC ${SUBSTRAIT_SOURCES} ${SUBSTRAIT_HEADERS})

target_link_libraries(substrait_og ${Protobuf_LIBRARIES})