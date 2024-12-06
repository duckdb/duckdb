#include <utility>

#include "duckdb/common/arrow/arrow_extension.hpp"
#include "duckdb/common/types/hash.hpp"

namespace duckdb {

ArrowExtensionInfo::ArrowExtensionInfo(string extension_name, string arrow_format)
    : extension_name(std::move(extension_name)), arrow_format(std::move(arrow_format)) {};

ArrowExtensionInfo::ArrowExtensionInfo(string vendor_name, string type_name, string arrow_format)
    : vendor_name(std::move(vendor_name)), type_name(std::move(type_name)), arrow_format(std::move(arrow_format)) {};

ArrowExtension::ArrowExtension(string extension_name, string arrow_format, shared_ptr<ArrowType> type)
    : extension_info(std::move(extension_name), std::move(arrow_format)), type(std::move(type)) {
}

hash_t ArrowExtensionInfo::GetHash() const {
	const auto h_extension = Hash(extension_name.c_str());
	const auto h_vendor = Hash(vendor_name.c_str());
	const auto h_type = Hash(type_name.c_str());
	const auto h_format = Hash(arrow_format.c_str());
	return CombineHash(h_extension, CombineHash(h_vendor, CombineHash(h_format, h_type)));
}
string ArrowExtensionInfo::ToString() const {
	std::ostringstream info;
	info << "Extension Name: " << extension_name << ", ";
	if (!vendor_name.empty()) {
		info << "Vendor: " << vendor_name << ", ";
	}
	if (!type_name.empty()) {
		info << "Type: " << type_name << ", ";
	}
	info << "Format: " << arrow_format << ". ";
	return info.str();
}

ArrowExtension::ArrowExtension(string vendor_name, string type_name, string arrow_format, shared_ptr<ArrowType> type)
    : extension_info(std::move(vendor_name), std::move(type_name), std::move(arrow_format)), type(std::move(type)) {
}

ArrowExtensionInfo ArrowExtension::GetInfo() const {
	return extension_info;
}

void ArrowExtensionSet::Initialize(DBConfig &config) {
	// config.RegisterEncodeFunction({"utf-8", DecodeUTF8, 1, 1});
	// config.RegisterEncodeFunction({"latin-1", DecodeLatin1ToUTF8, 2, 1});
	// config.RegisterEncodeFunction({"utf-16", DecodeUTF16ToUTF8, 2, 2});
}
} // namespace duckdb
