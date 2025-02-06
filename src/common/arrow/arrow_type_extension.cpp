#include "duckdb/common/arrow/arrow_type_extension.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

ArrowTypeExtension::ArrowTypeExtension(string extension_name, string arrow_format,
                                       shared_ptr<ArrowTypeExtensionData> type)
    : extension_metadata(std::move(extension_name), {}, {}, std::move(arrow_format)), type_extension(std::move(type)) {
}

ArrowTypeExtension::ArrowTypeExtension(ArrowExtensionMetadata &extension_metadata, unique_ptr<ArrowType> type)
    : extension_metadata(extension_metadata) {
	type_extension = make_shared_ptr<ArrowTypeExtensionData>(type->GetDuckType());
}

ArrowExtensionMetadata::ArrowExtensionMetadata(string extension_name, string vendor_name, string type_name,
                                               string arrow_format)
    : extension_name(std::move(extension_name)), vendor_name(std::move(vendor_name)), type_name(std::move(type_name)),
      arrow_format(std::move(arrow_format)) {
}

hash_t ArrowExtensionMetadata::GetHash() const {
	const auto h_extension = Hash(extension_name.c_str());
	const auto h_vendor = Hash(vendor_name.c_str());
	const auto h_type = Hash(type_name.c_str());
	// Most arrow extensions are unique on the extension name
	// However we use arrow.opaque as all the non-canonical extensions, hence we do a hash-aroo of all.
	return CombineHash(h_extension, CombineHash(h_vendor, h_type));
}

TypeInfo::TypeInfo() : type() {
}

TypeInfo::TypeInfo(const LogicalType &type_p) : alias(type_p.GetAlias()), type(type_p.id()) {
}

TypeInfo::TypeInfo(string alias) : alias(std::move(alias)), type(LogicalTypeId::ANY) {
}

hash_t TypeInfo::GetHash() const {
	const auto h_type_id = Hash(type);
	const auto h_alias = Hash(alias.c_str());
	return CombineHash(h_type_id, h_alias);
}

bool TypeInfo::operator==(const TypeInfo &other) const {
	return alias == other.alias && type == other.type;
}

string ArrowExtensionMetadata::ToString() const {
	std::ostringstream info;
	info << "Extension Name: " << extension_name << "\n";
	if (!vendor_name.empty()) {
		info << "Vendor: " << vendor_name << "\n";
	}
	if (!type_name.empty()) {
		info << "Type: " << type_name << "\n";
	}
	if (!arrow_format.empty()) {
		info << "Format: " << arrow_format << "\n";
	}
	return info.str();
}

string ArrowExtensionMetadata::GetExtensionName() const {
	return extension_name;
}

string ArrowExtensionMetadata::GetVendorName() const {
	return vendor_name;
}

string ArrowExtensionMetadata::GetTypeName() const {
	return type_name;
}

string ArrowExtensionMetadata::GetArrowFormat() const {
	return arrow_format;
}

void ArrowExtensionMetadata::SetArrowFormat(string arrow_format_p) {
	arrow_format = std::move(arrow_format_p);
}

bool ArrowExtensionMetadata::IsCanonical() const {
	D_ASSERT((!vendor_name.empty() && !type_name.empty()) || (vendor_name.empty() && type_name.empty()));
	return vendor_name.empty();
}

bool ArrowExtensionMetadata::operator==(const ArrowExtensionMetadata &other) const {
	return extension_name == other.extension_name && type_name == other.type_name && vendor_name == other.vendor_name;
}

ArrowTypeExtension::ArrowTypeExtension(string vendor_name, string type_name, string arrow_format,
                                       shared_ptr<ArrowTypeExtensionData> type)
    : extension_metadata(ArrowExtensionMetadata::ARROW_EXTENSION_NON_CANONICAL, std::move(vendor_name),
                         std::move(type_name), std::move(arrow_format)),
      type_extension(std::move(type)) {
}

ArrowTypeExtension::ArrowTypeExtension(string extension_name, populate_arrow_schema_t populate_arrow_schema,
                                       get_type_t get_type, shared_ptr<ArrowTypeExtensionData> type)
    : populate_arrow_schema(populate_arrow_schema), get_type(get_type),
      extension_metadata(std::move(extension_name), {}, {}, {}), type_extension(std::move(type)) {
}

ArrowTypeExtension::ArrowTypeExtension(string vendor_name, string type_name,
                                       populate_arrow_schema_t populate_arrow_schema, get_type_t get_type,
                                       shared_ptr<ArrowTypeExtensionData> type, cast_arrow_duck_t arrow_to_duckdb,
                                       cast_duck_arrow_t duckdb_to_arrow)
    : populate_arrow_schema(populate_arrow_schema), get_type(get_type),
      extension_metadata(ArrowExtensionMetadata::ARROW_EXTENSION_NON_CANONICAL, std::move(vendor_name),
                         std::move(type_name), {}),
      type_extension(std::move(type)) {
	type_extension->arrow_to_duckdb = arrow_to_duckdb;
	type_extension->duckdb_to_arrow = duckdb_to_arrow;
}

ArrowExtensionMetadata ArrowTypeExtension::GetInfo() const {
	return extension_metadata;
}

unique_ptr<ArrowType> ArrowTypeExtension::GetType(const ArrowSchema &schema,
                                                  const ArrowSchemaMetadata &schema_metadata) const {
	if (get_type) {
		return get_type(schema, schema_metadata);
	}
	// FIXME: THis is not good
	auto duckdb_type = type_extension->GetDuckDBType();
	return make_uniq<ArrowType>(duckdb_type);
}

shared_ptr<ArrowTypeExtensionData> ArrowTypeExtension::GetTypeExtension() const {
	return type_extension;
}

LogicalTypeId ArrowTypeExtension::GetLogicalTypeId() const {
	return type_extension->GetDuckDBType().id();
}

LogicalType ArrowTypeExtension::GetLogicalType() const {
	return type_extension->GetDuckDBType();
}

bool ArrowTypeExtension::HasType() const {
	return type_extension.get() != nullptr;
}

void ArrowTypeExtension::PopulateArrowSchema(DuckDBArrowSchemaHolder &root_holder, ArrowSchema &child,
                                             const LogicalType &duckdb_type, ClientContext &context,
                                             const ArrowTypeExtension &extension) {
	if (extension.populate_arrow_schema) {
		extension.populate_arrow_schema(root_holder, child, duckdb_type, context, extension);
		return;
	}

	auto format = make_unsafe_uniq_array<char>(extension.extension_metadata.GetArrowFormat().size() + 1);
	idx_t i = 0;
	for (const auto &c : extension.extension_metadata.GetArrowFormat()) {
		format[i++] = c;
	}
	format[i++] = '\0';
	// We do the default way of populating the schema
	root_holder.extension_format.emplace_back(std::move(format));

	child.format = root_holder.extension_format.back().get();
	ArrowSchemaMetadata schema_metadata;
	if (extension.extension_metadata.IsCanonical()) {
		schema_metadata = ArrowSchemaMetadata::ArrowCanonicalType(extension.extension_metadata.GetExtensionName());
	} else {
		schema_metadata = ArrowSchemaMetadata::NonCanonicalType(extension.extension_metadata.GetTypeName(),
		                                                        extension.extension_metadata.GetVendorName());
	}
	root_holder.metadata_info.emplace_back(schema_metadata.SerializeMetadata());
	child.metadata = root_holder.metadata_info.back().get();
}

void DBConfig::RegisterArrowExtension(const ArrowTypeExtension &extension) const {
	lock_guard<mutex> l(arrow_extensions->lock);
	auto extension_info = extension.GetInfo();
	if (arrow_extensions->type_extensions.find(extension_info) != arrow_extensions->type_extensions.end()) {
		throw NotImplementedException("Arrow Extension with configuration %s is already registered",
		                              extension_info.ToString());
	}
	arrow_extensions->type_extensions[extension_info] = extension;
	if (extension.HasType()) {
		const TypeInfo type_info(extension.GetLogicalType());
		arrow_extensions->type_to_info[type_info].push_back(extension_info);
		return;
	}
	const TypeInfo type_info(extension.GetInfo().GetExtensionName());
	arrow_extensions->type_to_info[type_info].push_back(extension_info);
}

ArrowTypeExtension GetArrowExtensionInternal(
    unordered_map<ArrowExtensionMetadata, ArrowTypeExtension, HashArrowTypeExtension> &type_extensions,
    ArrowExtensionMetadata info) {
	if (type_extensions.find(info) == type_extensions.end()) {
		auto og_info = info;
		info.SetArrowFormat("");
		if (type_extensions.find(info) == type_extensions.end()) {
			auto format = og_info.GetArrowFormat();
			auto type = ArrowType::GetTypeFromFormat(format);
			return ArrowTypeExtension(og_info, std::move(type));
		}
	}
	return type_extensions[info];
}
ArrowTypeExtension DBConfig::GetArrowExtension(ArrowExtensionMetadata info) const {
	lock_guard<mutex> l(arrow_extensions->lock);
	return GetArrowExtensionInternal(arrow_extensions->type_extensions, std::move(info));
}

ArrowTypeExtension DBConfig::GetArrowExtension(const LogicalType &type) const {
	lock_guard<mutex> l(arrow_extensions->lock);
	TypeInfo type_info(type);
	if (!arrow_extensions->type_to_info[type_info].empty()) {
		return GetArrowExtensionInternal(arrow_extensions->type_extensions,
		                                 arrow_extensions->type_to_info[type_info].front());
	}
	type_info.type = LogicalTypeId::ANY;
	return GetArrowExtensionInternal(arrow_extensions->type_extensions,
	                                 arrow_extensions->type_to_info[type_info].front());
}

bool DBConfig::HasArrowExtension(const LogicalType &type) const {
	lock_guard<mutex> l(arrow_extensions->lock);
	TypeInfo type_info(type);
	if (!arrow_extensions->type_to_info[type_info].empty()) {
		return true;
	}
	type_info.type = LogicalTypeId::ANY;
	return !arrow_extensions->type_to_info[type_info].empty();
}

bool DBConfig::HasArrowExtension(ArrowExtensionMetadata info) const {
	lock_guard<mutex> l(arrow_extensions->lock);
	auto type_extensions = arrow_extensions->type_extensions;

	if (type_extensions.find(info) != type_extensions.end()) {
		return true;
	}

	auto og_info = info;
	info.SetArrowFormat("");
	if (type_extensions.find(info) != type_extensions.end()) {
		return true;
	}

	return false;
}

struct ArrowJson {
	static unique_ptr<ArrowType> GetType(const ArrowSchema &schema, const ArrowSchemaMetadata &schema_metadata) {
		const auto format = string(schema.format);
		if (format == "u") {
			return make_uniq<ArrowType>(LogicalType::JSON(), make_uniq<ArrowStringInfo>(ArrowVariableSizeType::NORMAL));
		} else if (format == "U") {
			return make_uniq<ArrowType>(LogicalType::JSON(),
			                            make_uniq<ArrowStringInfo>(ArrowVariableSizeType::SUPER_SIZE));
		} else if (format == "vu") {
			return make_uniq<ArrowType>(LogicalType::JSON(), make_uniq<ArrowStringInfo>(ArrowVariableSizeType::VIEW));
		}
		throw InvalidInputException("Arrow extension type \"%s\" not supported for arrow.json", format.c_str());
	}

	static void PopulateSchema(DuckDBArrowSchemaHolder &root_holder, ArrowSchema &schema, const LogicalType &type,
	                           ClientContext &context, const ArrowTypeExtension &extension) {
		const ArrowSchemaMetadata schema_metadata =
		    ArrowSchemaMetadata::ArrowCanonicalType(extension.GetInfo().GetExtensionName());
		root_holder.metadata_info.emplace_back(schema_metadata.SerializeMetadata());
		schema.metadata = root_holder.metadata_info.back().get();
		const auto options = context.GetClientProperties();
		if (options.produce_arrow_string_view) {
			schema.format = "vu";
		} else {
			if (options.arrow_offset_size == ArrowOffsetSize::LARGE) {
				schema.format = "U";
			} else {
				schema.format = "u";
			}
		}
	}
};

struct ArrowBit {
	static unique_ptr<ArrowType> GetType(const ArrowSchema &schema, const ArrowSchemaMetadata &schema_metadata) {
		const auto format = string(schema.format);
		if (format == "z") {
			return make_uniq<ArrowType>(LogicalType::BIT, make_uniq<ArrowStringInfo>(ArrowVariableSizeType::NORMAL));
		} else if (format == "Z") {
			return make_uniq<ArrowType>(LogicalType::BIT,
			                            make_uniq<ArrowStringInfo>(ArrowVariableSizeType::SUPER_SIZE));
		}
		throw InvalidInputException("Arrow extension type \"%s\" not supported for BIT type", format.c_str());
	}

	static void PopulateSchema(DuckDBArrowSchemaHolder &root_holder, ArrowSchema &schema, const LogicalType &type,
	                           ClientContext &context, const ArrowTypeExtension &extension) {
		const ArrowSchemaMetadata schema_metadata = ArrowSchemaMetadata::NonCanonicalType(
		    extension.GetInfo().GetTypeName(), extension.GetInfo().GetVendorName());
		root_holder.metadata_info.emplace_back(schema_metadata.SerializeMetadata());
		schema.metadata = root_holder.metadata_info.back().get();
		const auto options = context.GetClientProperties();
		if (options.arrow_offset_size == ArrowOffsetSize::LARGE) {
			schema.format = "Z";
		} else {
			schema.format = "z";
		}
	}
};

struct ArrowVarint {
	static unique_ptr<ArrowType> GetType(const ArrowSchema &schema, const ArrowSchemaMetadata &schema_metadata) {
		const auto format = string(schema.format);
		if (format == "z") {
			return make_uniq<ArrowType>(LogicalType::VARINT, make_uniq<ArrowStringInfo>(ArrowVariableSizeType::NORMAL));
		} else if (format == "Z") {
			return make_uniq<ArrowType>(LogicalType::VARINT,
			                            make_uniq<ArrowStringInfo>(ArrowVariableSizeType::SUPER_SIZE));
		}
		throw InvalidInputException("Arrow extension type \"%s\" not supported for Varint", format.c_str());
	}

	static void PopulateSchema(DuckDBArrowSchemaHolder &root_holder, ArrowSchema &schema, const LogicalType &type,
	                           ClientContext &context, const ArrowTypeExtension &extension) {
		const ArrowSchemaMetadata schema_metadata = ArrowSchemaMetadata::NonCanonicalType(
		    extension.GetInfo().GetTypeName(), extension.GetInfo().GetVendorName());
		root_holder.metadata_info.emplace_back(schema_metadata.SerializeMetadata());
		schema.metadata = root_holder.metadata_info.back().get();
		const auto options = context.GetClientProperties();
		if (options.arrow_offset_size == ArrowOffsetSize::LARGE) {
			schema.format = "Z";
		} else {
			schema.format = "z";
		}
	}
};

struct ArrowBool8 {
	static void ArrowToDuck(ClientContext &context, Vector &source, Vector &result, idx_t count) {
		auto source_ptr = reinterpret_cast<int8_t *>(FlatVector::GetData(source));
		auto result_ptr = reinterpret_cast<bool *>(FlatVector::GetData(result));
		for (idx_t i = 0; i < count; i++) {
			result_ptr[i] = source_ptr[i];
		}
	}
	static void DuckToArrow(ClientContext &context, Vector &source, Vector &result, idx_t count) {
		UnifiedVectorFormat format;
		source.ToUnifiedFormat(count, format);
		FlatVector::SetValidity(result, format.validity);
		auto source_ptr = reinterpret_cast<bool *>(format.data);
		auto result_ptr = reinterpret_cast<int8_t *>(FlatVector::GetData(result));
		for (idx_t i = 0; i < count; i++) {
			result_ptr[i] = static_cast<int8_t>(source_ptr[i]);
		}
	}
};

void ArrowTypeExtensionSet::Initialize(const DBConfig &config) {
	// Types that are 1:1
	config.RegisterArrowExtension({"arrow.uuid", "w:16", make_shared_ptr<ArrowTypeExtensionData>(LogicalType::UUID)});
	config.RegisterArrowExtension(
	    {"arrow.bool8", "c",
	     make_shared_ptr<ArrowTypeExtensionData>(LogicalType::BOOLEAN, LogicalType::TINYINT, ArrowBool8::ArrowToDuck,
	                                             ArrowBool8::DuckToArrow)});

	config.RegisterArrowExtension(
	    {"DuckDB", "hugeint", "w:16", make_shared_ptr<ArrowTypeExtensionData>(LogicalType::HUGEINT)});
	config.RegisterArrowExtension(
	    {"DuckDB", "uhugeint", "w:16", make_shared_ptr<ArrowTypeExtensionData>(LogicalType::UHUGEINT)});
	config.RegisterArrowExtension(
	    {"DuckDB", "time_tz", "w:8", make_shared_ptr<ArrowTypeExtensionData>(LogicalType::TIME_TZ)});

	// Types that are 1:n
	config.RegisterArrowExtension({"arrow.json", &ArrowJson::PopulateSchema, &ArrowJson::GetType,
	                               make_shared_ptr<ArrowTypeExtensionData>(LogicalType::VARCHAR)});

	config.RegisterArrowExtension({"DuckDB", "bit", &ArrowBit::PopulateSchema, &ArrowBit::GetType,
	                               make_shared_ptr<ArrowTypeExtensionData>(LogicalType::BIT), nullptr, nullptr});

	config.RegisterArrowExtension({"DuckDB", "varint", &ArrowVarint::PopulateSchema, &ArrowVarint::GetType,
	                               make_shared_ptr<ArrowTypeExtensionData>(LogicalType::VARINT), nullptr, nullptr});
}
} // namespace duckdb
