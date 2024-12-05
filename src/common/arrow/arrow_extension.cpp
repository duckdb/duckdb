#include <utility>

#include "duckdb/common/arrow/arrow_extension.hpp"
namespace duckdb {
ArrowExtension::ArrowExtension(string extension_name, string arrow_format)
    : extension_name(std::move(extension_name)), arrow_format(std::move(arrow_format)) {
}

ArrowExtension::ArrowExtension(string vendor_name, string type_name, string arrow_format)
    : extension_name(ARROW_EXTENSION_NON_CANONICAL), vendor_name(std::move(vendor_name)),
      type_name(std::move(type_name)), arrow_format(std::move(arrow_format)) {
}

unique_ptr<ArrowType> ArrowExtension::GetArrowExtensionType() {
}
} // namespace duckdb
