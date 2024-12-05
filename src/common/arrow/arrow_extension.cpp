#include "duckdb/common/arrow/arrow_extension.hpp"
#include "duckdb/common/types/hash.hpp"

namespace duckdb {
ArrowExtension::ArrowExtension(string extension_name, string arrow_format)
    : extension_name(std::move(extension_name)), arrow_format(std::move(arrow_format)) {
}

ArrowExtension::ArrowExtension(string vendor_name, string type_name, string arrow_format)
    : extension_name(ARROW_EXTENSION_NON_CANONICAL), vendor_name(std::move(vendor_name)),
      type_name(std::move(type_name)), arrow_format(std::move(arrow_format)) {
}

unique_ptr<ArrowType> ArrowExtension::GetArrowExtensionType() const {
	auto type = ArrowType::GetTypeFromFormat(arrow_format);
	if (!type) {
		throw NotImplementedException("ArrowExtensionType with %s format is not supported yet", arrow_format.c_str());
	}
	return type;
}

hash_t ArrowExtension::GetHash() const {
	auto h_extension = Hash(extension_name.c_str());
	auto h_vendor = Hash(vendor_name.c_str());
	auto h_type = Hash(type_name.c_str());
	return CombineHash(h_extension, CombineHash(h_vendor, h_type));
}
} // namespace duckdb
