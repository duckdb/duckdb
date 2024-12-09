#include "duckdb/common/arrow/arrow_extension.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"

namespace duckdb {

ArrowExtensionInfo::ArrowExtensionInfo(string extension_name, string arrow_format)
    : extension_name(std::move(extension_name)), arrow_format(std::move(arrow_format)) {};

ArrowExtensionInfo::ArrowExtensionInfo(string vendor_name, string type_name, string arrow_format)
    : vendor_name(std::move(vendor_name)), type_name(std::move(type_name)), arrow_format(std::move(arrow_format)) {};

ArrowExtension::ArrowExtension(string extension_name, string arrow_format, shared_ptr<ArrowType> type)
    : extension_info(std::move(extension_name), std::move(arrow_format)), type(std::move(type)) {
}

ArrowExtensionInfo::ArrowExtensionInfo(string extension_name, string vendor_name, string type_name, string arrow_format)
    : extension_name(std::move(extension_name)), vendor_name(std::move(vendor_name)), type_name(std::move(type_name)),
      arrow_format(std::move(arrow_format)) {};

hash_t ArrowExtensionInfo::GetHash() const {
	const auto h_extension = Hash(extension_name.c_str());
	const auto h_vendor = Hash(vendor_name.c_str());
	const auto h_type = Hash(type_name.c_str());
	const auto h_format = Hash(arrow_format.c_str());
	return CombineHash(h_extension, CombineHash(h_vendor, CombineHash(h_format, h_type)));
}

TypeInfo::TypeInfo() : type() {
}

TypeInfo::TypeInfo(const LogicalType &type_p) : alias(type_p.GetAlias()), type(type_p.id()) {
}

hash_t TypeInfo::GetHash() const {
	const auto h_type_id = Hash(type);
	const auto h_alias = Hash(alias.c_str());
	return CombineHash(h_type_id, h_alias);
}

bool TypeInfo::operator==(const TypeInfo &other) const {
	return alias == other.alias && type == other.type;
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

string ArrowExtensionInfo::GetExtensionName() const {
	return extension_name;
}

string ArrowExtensionInfo::GetVendorName() const {
	return vendor_name;
}

string ArrowExtensionInfo::GetTypeName() const {
	return type_name;
}

string ArrowExtensionInfo::GetArrowFormat() const {
	return arrow_format;
}

bool ArrowExtensionInfo::IsCanonical() const {
	D_ASSERT((!vendor_name.empty() && !type_name.empty()) || (vendor_name.empty() && type_name.empty()));
	return vendor_name.empty();
}

bool ArrowExtensionInfo::operator==(const ArrowExtensionInfo &other) const {
	return extension_name == other.extension_name && type_name == other.type_name &&
	       arrow_format == other.arrow_format && vendor_name == other.vendor_name;
}

ArrowExtension::ArrowExtension(string vendor_name, string type_name, string arrow_format, shared_ptr<ArrowType> type)
    : extension_info(std::move(vendor_name), std::move(type_name), std::move(arrow_format)), type(std::move(type)) {
}

ArrowExtensionInfo ArrowExtension::GetInfo() const {
	return extension_info;
}

shared_ptr<ArrowType> ArrowExtension::GetType() const {
	return type;
}

LogicalTypeId ArrowExtension::GetLogicalTypeId() const {
	return type->GetDuckType().id();
}

LogicalType ArrowExtension::GetLogicalType() const {
	return type->GetDuckType();
}

void DBConfig::RegisterArrowExtension(const ArrowExtension &extension) const {
	lock_guard<mutex> l(encoding_functions->lock);
	auto extension_info = extension.GetInfo();
	if (arrow_extensions->extensions.find(extension_info) != arrow_extensions->extensions.end()) {
		throw InvalidInputException("Arrow Extension with configuration %s is already registered",
		                            extension_info.ToString());
	}
	arrow_extensions->extensions[extension_info] = extension;
	const TypeInfo type_info(extension.GetLogicalType());
	arrow_extensions->type_to_info[type_info].push_back(extension_info);
}

ArrowExtension DBConfig::GetArrowExtension(const ArrowExtensionInfo &info) const {
	if (arrow_extensions->extensions.find(info) != arrow_extensions->extensions.end()) {
		throw InvalidInputException("Arrow Extension with configuration %s is not yet registered", info.ToString());
	}
	return arrow_extensions->extensions[info];
}

ArrowExtension DBConfig::GetArrowExtension(const LogicalType &type) const {
	const TypeInfo type_info(type);
	return GetArrowExtension(arrow_extensions->type_to_info[type_info].front());
}

bool DBConfig::HasArrowExtension(const LogicalType &type) const {
	const TypeInfo type_info(type);
	return !arrow_extensions->type_to_info[type_info].empty();
}

void ArrowExtensionSet::Initialize(DBConfig &config) {
	config.RegisterArrowExtension({"arrow.uuid", "w:16", make_shared_ptr<ArrowType>(LogicalType::UUID)});
	config.RegisterArrowExtension(
	    {"arrow.json", "u",
	     make_shared_ptr<ArrowType>(LogicalType::JSON(), make_uniq<ArrowStringInfo>(ArrowVariableSizeType::NORMAL))});
	config.RegisterArrowExtension(
	    {"arrow.json", "U",
	     make_shared_ptr<ArrowType>(LogicalType::JSON(),
	                                make_uniq<ArrowStringInfo>(ArrowVariableSizeType::SUPER_SIZE))});
	config.RegisterArrowExtension(
	    {"arrow.json", "vu",
	     make_shared_ptr<ArrowType>(LogicalType::JSON(), make_uniq<ArrowStringInfo>(ArrowVariableSizeType::VIEW))});
	config.RegisterArrowExtension({"hugeint", "DuckDB", "w:16", make_shared_ptr<ArrowType>(LogicalType::HUGEINT)});
	config.RegisterArrowExtension({"uhugeint", "DuckDB", "w:16", make_shared_ptr<ArrowType>(LogicalType::UHUGEINT)});
	config.RegisterArrowExtension(
	    {"time_tz", "DuckDB", "w:8",
	     make_shared_ptr<ArrowType>(LogicalType::TIME_TZ,
	                                make_uniq<ArrowDateTimeInfo>(ArrowDateTimeType::MICROSECONDS))});
	config.RegisterArrowExtension(
	    {"bit", "DuckDB", "z",
	     make_shared_ptr<ArrowType>(LogicalType::BIT, make_uniq<ArrowStringInfo>(ArrowVariableSizeType::NORMAL))});
	config.RegisterArrowExtension(
	    {"bit", "DuckDB", "Z",
	     make_shared_ptr<ArrowType>(LogicalType::BIT, make_uniq<ArrowStringInfo>(ArrowVariableSizeType::SUPER_SIZE))});
	config.RegisterArrowExtension(
	    {"varint", "DuckDB", "z",
	     make_shared_ptr<ArrowType>(LogicalType::VARINT, make_uniq<ArrowStringInfo>(ArrowVariableSizeType::NORMAL))});
	config.RegisterArrowExtension(
	    {"varint", "DuckDB", "Z",
	     make_shared_ptr<ArrowType>(LogicalType::VARINT,
	                                make_uniq<ArrowStringInfo>(ArrowVariableSizeType::SUPER_SIZE))});
}
} // namespace duckdb
