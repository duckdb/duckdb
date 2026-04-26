#include "duckdb/common/arrow/arrow_type_extension.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/types/geometry_crs.hpp"

#include "yyjson.hpp"

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

unique_ptr<ArrowType> ArrowTypeExtension::GetType(ClientContext &context, const ArrowSchema &schema,
                                                  const ArrowSchemaMetadata &schema_metadata) const {
	if (get_type) {
		return get_type(context, schema, schema_metadata);
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
	static unique_ptr<ArrowType> GetType(ClientContext &context, const ArrowSchema &schema,
	                                     const ArrowSchemaMetadata &schema_metadata) {
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
	static unique_ptr<ArrowType> GetType(ClientContext &context, const ArrowSchema &schema,
	                                     const ArrowSchemaMetadata &schema_metadata) {
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

struct ArrowBignum {
	static unique_ptr<ArrowType> GetType(ClientContext &context, const ArrowSchema &schema,
	                                     const ArrowSchemaMetadata &schema_metadata) {
		const auto format = string(schema.format);
		if (format == "z") {
			return make_uniq<ArrowType>(LogicalType::BIGNUM, make_uniq<ArrowStringInfo>(ArrowVariableSizeType::NORMAL));
		} else if (format == "Z") {
			return make_uniq<ArrowType>(LogicalType::BIGNUM,
			                            make_uniq<ArrowStringInfo>(ArrowVariableSizeType::SUPER_SIZE));
		}
		throw InvalidInputException("Arrow extension type \"%s\" not supported for Bignum", format.c_str());
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
		auto source_ptr = FlatVector::GetData<int8_t>(source);
		auto result_ptr = FlatVector::GetDataMutable<bool>(result);
		for (idx_t i = 0; i < count; i++) {
			result_ptr[i] = source_ptr[i];
		}
	}
	static void DuckToArrow(ClientContext &context, Vector &source, Vector &result, idx_t count) {
		auto entries = source.Values<bool>(count);
		auto &result_validity = FlatVector::ValidityMutable(result);
		auto result_ptr = FlatVector::GetDataMutable<int8_t>(result);
		for (idx_t i = 0; i < count; i++) {
			auto entry = entries[i];
			if (entry.IsValid()) {
				result_ptr[i] = static_cast<int8_t>(entry.GetValue());
			} else {
				result_validity.SetInvalid(i);
			}
		}
	}
};

struct ArrowGeometry {
	static unique_ptr<ArrowType> GetType(ClientContext &context, const ArrowSchema &schema,
	                                     const ArrowSchemaMetadata &schema_metadata) {
		// Validate extension metadata. This metadata also contains a CRS, which we drop
		// because the GEOMETRY type does not implement a CRS at the type level (yet).
		const auto extension_metadata = schema_metadata.GetOption(ArrowSchemaMetadata::ARROW_METADATA_KEY);

		unique_ptr<CoordinateReferenceSystem> duckdb_crs;

		if (!extension_metadata.empty()) {
			unique_ptr<duckdb_yyjson::yyjson_doc, void (*)(duckdb_yyjson::yyjson_doc *)> doc(
			    duckdb_yyjson::yyjson_read(extension_metadata.data(), extension_metadata.size(),
			                               duckdb_yyjson::YYJSON_READ_NOFLAG),
			    duckdb_yyjson::yyjson_doc_free);
			if (!doc) {
				throw SerializationException("Invalid JSON in GeoArrow metadata");
			}

			duckdb_yyjson::yyjson_val *val = yyjson_doc_get_root(doc.get());
			if (!yyjson_is_obj(val)) {
				throw SerializationException("Invalid GeoArrow metadata: not a JSON object");
			}

			duckdb_yyjson::yyjson_val *edges = yyjson_obj_get(val, "edges");
			if (edges && yyjson_is_str(edges) && std::strcmp(yyjson_get_str(edges), "planar") != 0) {
				throw NotImplementedException("Can't import non-planar edges");
			}

			// Pick out the CRS if present
			duckdb_yyjson::yyjson_val *crs = yyjson_obj_get(val, "crs");

			if (crs) {
				if (duckdb_yyjson::yyjson_is_str(crs)) {
					const char *crs_str = duckdb_yyjson::yyjson_get_str(crs);
					duckdb_crs = CoordinateReferenceSystem::TryIdentify(context, crs_str);
				} else if (duckdb_yyjson::yyjson_is_obj(crs)) {
					// Stringify the object
					duckdb_yyjson::yyjson_write_flag write_flags = duckdb_yyjson::YYJSON_WRITE_NOFLAG;
					size_t len = 0;
					const auto crs_str = duckdb_yyjson::yyjson_val_write(crs, write_flags, &len);
					if (crs_str) {
						const auto str = string(crs_str, len);
						free(crs_str);
						duckdb_crs = CoordinateReferenceSystem::TryIdentify(context, str);
					} else {
						throw SerializationException("Could not serialize CRS object from GeoArrow metadata");
					}
				}
			}
		}

		// Create the geometry type, with or without CRS
		auto geo_type = duckdb_crs ? LogicalType::GEOMETRY(*duckdb_crs) : LogicalType::GEOMETRY();

		const auto format = string(schema.format);
		if (format == "z") {
			return make_uniq<ArrowType>(std::move(geo_type), make_uniq<ArrowStringInfo>(ArrowVariableSizeType::NORMAL));
		}
		if (format == "Z") {
			return make_uniq<ArrowType>(std::move(geo_type),
			                            make_uniq<ArrowStringInfo>(ArrowVariableSizeType::SUPER_SIZE));
		}
		if (format == "vz") {
			return make_uniq<ArrowType>(std::move(geo_type), make_uniq<ArrowStringInfo>(ArrowVariableSizeType::VIEW));
		}
		throw InvalidInputException("Arrow extension type \"%s\" not supported for geoarrow.wkb", format.c_str());
	}

	static void WriteCRS(duckdb_yyjson::yyjson_mut_doc *doc, const CoordinateReferenceSystem &crs,
	                     ClientContext &context) {
		// Try to convert to preferred formats, in order
		auto converted = CoordinateReferenceSystem::TryConvert(context, crs, CoordinateReferenceSystemType::PROJJSON);
		if (!converted) {
			converted = CoordinateReferenceSystem::TryConvert(context, crs, CoordinateReferenceSystemType::WKT2_2019);
		}
		if (!converted) {
			converted = CoordinateReferenceSystem::TryConvert(context, crs, CoordinateReferenceSystemType::AUTH_CODE);
		}
		if (!converted) {
			converted = CoordinateReferenceSystem::TryConvert(context, crs, CoordinateReferenceSystemType::SRID);
		}
		if (!converted) {
			converted = nullptr;
		}

		const auto &crs_def = converted ? converted->GetDefinition() : crs.GetDefinition();
		const auto &crs_type = converted ? converted->GetType() : crs.GetType();

		const auto root = duckdb_yyjson::yyjson_mut_doc_get_root(doc);

		switch (crs_type) {
		case CoordinateReferenceSystemType::PROJJSON: {
			const auto projjson_doc =
			    duckdb_yyjson::yyjson_read(crs_def.c_str(), crs_def.size(), duckdb_yyjson::YYJSON_READ_NOFLAG);
			if (projjson_doc) {
				const auto projjson_val = duckdb_yyjson::yyjson_doc_get_root(projjson_doc);
				const auto projjson_obj = duckdb_yyjson::yyjson_val_mut_copy(doc, projjson_val);

				duckdb_yyjson::yyjson_mut_obj_add_str(doc, root, "crs_type", "projjson");
				duckdb_yyjson::yyjson_mut_obj_add_val(doc, root, "crs", projjson_obj);

				duckdb_yyjson::yyjson_doc_free(projjson_doc);
			} else {
				duckdb_yyjson::yyjson_mut_doc_free(doc);
				throw SerializationException("Could not parse PROJJSON CRS for GeoArrow metadata");
			}
		} break;
		case CoordinateReferenceSystemType::AUTH_CODE: {
			duckdb_yyjson::yyjson_mut_obj_add_str(doc, root, "crs_type", "authority_code");
			duckdb_yyjson::yyjson_mut_obj_add_str(doc, root, "crs", crs_def.c_str());
		} break;
		case CoordinateReferenceSystemType::SRID: {
			duckdb_yyjson::yyjson_mut_obj_add_str(doc, root, "crs_type", "srid");
			duckdb_yyjson::yyjson_mut_obj_add_str(doc, root, "crs", crs_def.c_str());
		} break;
		case CoordinateReferenceSystemType::WKT2_2019: {
			duckdb_yyjson::yyjson_mut_obj_add_str(doc, root, "crs_type", "wkt2:2019");
			duckdb_yyjson::yyjson_mut_obj_add_str(doc, root, "crs", crs_def.c_str());
		} break;
		default:
			throw SerializationException("Could not serialize CRS of type %d for GeoArrow metadata",
			                             static_cast<int>(crs.GetType()));
		}
	}

	static void PopulateSchema(DuckDBArrowSchemaHolder &root_holder, ArrowSchema &schema, const LogicalType &type,
	                           ClientContext &context, const ArrowTypeExtension &extension) {
		ArrowSchemaMetadata schema_metadata;

		schema_metadata.AddOption(ArrowSchemaMetadata::ARROW_EXTENSION_NAME, "geoarrow.wkb");

		// Make a CRS entry if the type has a CRS
		const auto doc = duckdb_yyjson::yyjson_mut_doc_new(nullptr);
		const auto root = duckdb_yyjson::yyjson_mut_obj(doc);
		duckdb_yyjson::yyjson_mut_doc_set_root(doc, root);

		if (GeoType::HasCRS(type)) {
			try {
				WriteCRS(doc, GeoType::GetCRS(type), context);
			} catch (...) {
				duckdb_yyjson::yyjson_mut_doc_free(doc);
				throw;
			}
		}

		size_t json_size = 0;
		const auto json_text = duckdb_yyjson::yyjson_mut_write(doc, duckdb_yyjson::YYJSON_WRITE_NOFLAG, &json_size);
		if (json_text) {
			schema_metadata.AddOption(ArrowSchemaMetadata::ARROW_METADATA_KEY, json_text);
			duckdb_yyjson::yyjson_mut_doc_free(doc);
			free(json_text);
		} else {
			schema_metadata.AddOption(ArrowSchemaMetadata::ARROW_METADATA_KEY, "{}");
		}

		root_holder.metadata_info.emplace_back(schema_metadata.SerializeMetadata());
		schema.metadata = root_holder.metadata_info.back().get();

		const auto options = context.GetClientProperties();
		if (options.arrow_offset_size == ArrowOffsetSize::LARGE) {
			schema.format = "Z";
		} else {
			schema.format = "z";
		}
	}

	static void ArrowToDuck(ClientContext &, Vector &source, Vector &result, idx_t count) {
		Geometry::FromBinary(source, result, count, true);
	}

	static void DuckToArrow(ClientContext &context, Vector &source, Vector &result, idx_t count) {
		Geometry::ToBinary(source, result, count);
	}
};

struct ArrowFixedShapeTensor {
	struct TensorMetadata {
		vector<idx_t> shape;
		vector<idx_t> permutation; // empty means row-major (identity)
	};

	static TensorMetadata ParseMetadata(const string &metadata_str) {
		TensorMetadata meta;
		if (metadata_str.empty()) {
			throw InvalidInputException("arrow.fixed_shape_tensor: missing extension metadata");
		}
		unique_ptr<duckdb_yyjson::yyjson_doc, void (*)(duckdb_yyjson::yyjson_doc *)> doc(
		    duckdb_yyjson::yyjson_read(metadata_str.data(), metadata_str.size(), duckdb_yyjson::YYJSON_READ_NOFLAG),
		    duckdb_yyjson::yyjson_doc_free);
		if (!doc) {
			throw InvalidInputException("arrow.fixed_shape_tensor: invalid JSON metadata");
		}
		auto *root = duckdb_yyjson::yyjson_doc_get_root(doc.get());
		if (!duckdb_yyjson::yyjson_is_obj(root)) {
			throw InvalidInputException("arrow.fixed_shape_tensor: metadata must be a JSON object");
		}
		auto *shape_val = duckdb_yyjson::yyjson_obj_get(root, "shape");
		if (!shape_val || !duckdb_yyjson::yyjson_is_arr(shape_val)) {
			throw InvalidInputException("arrow.fixed_shape_tensor: metadata must contain a 'shape' array");
		}
		size_t idx, max;
		duckdb_yyjson::yyjson_val *dim;
		yyjson_arr_foreach(shape_val, idx, max, dim) {
			if (!duckdb_yyjson::yyjson_is_int(dim) && !duckdb_yyjson::yyjson_is_uint(dim)) {
				throw InvalidInputException("arrow.fixed_shape_tensor: shape dimensions must be integers");
			}
			meta.shape.push_back(NumericCast<idx_t>(duckdb_yyjson::yyjson_get_uint(dim)));
		}
		if (meta.shape.empty()) {
			throw InvalidInputException("arrow.fixed_shape_tensor: shape must have at least one dimension");
		}

		// Parse optional permutation field
		auto *perm_val = duckdb_yyjson::yyjson_obj_get(root, "permutation");
		if (perm_val && duckdb_yyjson::yyjson_is_arr(perm_val)) {
			duckdb_yyjson::yyjson_val *p;
			yyjson_arr_foreach(perm_val, idx, max, p) {
				if (!duckdb_yyjson::yyjson_is_int(p) && !duckdb_yyjson::yyjson_is_uint(p)) {
					throw InvalidInputException("arrow.fixed_shape_tensor: permutation values must be integers");
				}
				meta.permutation.push_back(NumericCast<idx_t>(duckdb_yyjson::yyjson_get_uint(p)));
			}
			if (meta.permutation.size() != meta.shape.size()) {
				throw InvalidInputException(
				    "arrow.fixed_shape_tensor: permutation length (%llu) must match shape length (%llu)",
				    meta.permutation.size(), meta.shape.size());
			}
			// Validate it's a valid permutation of [0..n-1]
			vector<bool> seen(meta.shape.size(), false);
			for (auto v : meta.permutation) {
				if (v >= meta.shape.size() || seen[v]) {
					throw InvalidInputException(
					    "arrow.fixed_shape_tensor: permutation must be a valid permutation of [0..%llu]",
					    meta.shape.size() - 1);
				}
				seen[v] = true;
			}
			// Check if permutation is identity — if so, clear it
			bool is_identity = true;
			for (idx_t i = 0; i < meta.permutation.size(); i++) {
				if (meta.permutation[i] != i) {
					is_identity = false;
					break;
				}
			}
			if (is_identity) {
				meta.permutation.clear();
			}
		}

		return meta;
	}

	static LogicalType BuildNestedArrayType(const LogicalType &element_type, const vector<idx_t> &shape) {
		auto result = element_type;
		for (int i = NumericCast<int>(shape.size()) - 1; i >= 0; i--) {
			result = LogicalType::ARRAY(result, shape[i]);
		}
		return result;
	}

	//! Encode permutation as an alias string "perm:1,0,2" on the internal type,
	//! so ArrowToDuck can read it from the source vector's type without needing
	//! extra state passed through the function pointer callback.
	static string EncodePermutation(const vector<idx_t> &permutation) {
		string result = "perm:";
		for (idx_t i = 0; i < permutation.size(); i++) {
			if (i > 0) {
				result += ",";
			}
			result += to_string(permutation[i]);
		}
		return result;
	}

	static vector<idx_t> DecodePermutation(const string &alias) {
		vector<idx_t> perm;
		// alias is "perm:1,0,2" etc.
		auto values = alias.substr(5); // skip "perm:"
		idx_t start = 0;
		for (idx_t i = 0; i <= values.size(); i++) {
			if (i == values.size() || values[i] == ',') {
				perm.push_back(NumericCast<idx_t>(StringUtil::ToSigned(values.substr(start, i - start))));
				start = i + 1;
			}
		}
		return perm;
	}

	static unique_ptr<ArrowType> GetType(ClientContext &context, const ArrowSchema &schema,
	                                     const ArrowSchemaMetadata &schema_metadata) {
		auto metadata_str = schema_metadata.GetOption(ArrowSchemaMetadata::ARROW_METADATA_KEY);
		auto meta = ParseMetadata(metadata_str);

		// Parse the format to get the flat size: "+w:NN"
		auto format = string(schema.format);
		if (format.size() <= 3 || format[0] != '+' || format[1] != 'w' || format[2] != ':') {
			throw InvalidInputException("arrow.fixed_shape_tensor: expected FixedSizeList format (+w:N), got '%s'",
			                            format);
		}
		auto flat_size = NumericCast<idx_t>(StringUtil::ToSigned(format.substr(3)));

		// Validate product(shape) == flat_size
		idx_t product = 1;
		for (auto d : meta.shape) {
			product *= d;
		}
		if (product != flat_size) {
			throw InvalidInputException(
			    "arrow.fixed_shape_tensor: shape product (%llu) doesn't match FixedSizeList size (%llu)", product,
			    flat_size);
		}

		// Get element type from child schema
		auto child_arrow_type = ArrowType::GetArrowLogicalType(context, *schema.children[0]);
		auto element_type = child_arrow_type->GetDuckType();

		// Build the nested ARRAY type from shape
		auto nested_type = BuildNestedArrayType(element_type, meta.shape);
		// The flat internal type (what the arrow reader produces)
		auto flat_type = LogicalType::ARRAY(element_type, flat_size);

		// If permutation is non-identity, encode it in the internal type's alias
		// so ArrowToDuck can access it through the source vector's type
		if (!meta.permutation.empty()) {
			flat_type.SetAlias(EncodePermutation(meta.permutation));
		}

		// Create per-column extension data with correct internal type and conversion callback
		auto ext_data = make_shared_ptr<ArrowTypeExtensionData>(nested_type, flat_type, ArrowToDuck, DuckToArrow);

		// Create the ArrowType for the flat FixedSizeList (used by the reader)
		auto type_info = make_uniq<ArrowArrayInfo>(std::move(child_arrow_type), flat_size);
		auto result = make_uniq<ArrowType>(nested_type, std::move(type_info));
		result->extension_data = std::move(ext_data);
		return result;
	}

	static void ArrowToDuck(ClientContext &context, Vector &source, Vector &result, idx_t count) {
		// source: flat ARRAY(T, N) where N = product(shape)
		// result: nested ARRAY(ARRAY(..., shape[-1]), shape[-2]) etc.
		auto flat_size = ArrayType::GetSize(source.GetType());

		// Check if permutation is encoded in the source type's alias
		auto alias = source.GetType().GetAlias();
		bool has_permutation = alias.size() >= 5 && alias.substr(0, 5) == "perm:";

		if (!has_permutation) {
			// Row-major (identity permutation): flat data is already in the right order.
			// Nested DuckDB arrays are stored contiguously, so just copy leaf data.
			Vector *dst = &result;
			while (ArrayType::GetChildType(dst->GetType()).id() == LogicalTypeId::ARRAY) {
				dst = &ArrayVector::GetChildMutable(*dst);
			}
			auto &dst_leaf = ArrayVector::GetChildMutable(*dst);
			auto &src_leaf = ArrayVector::GetChildMutable(source);
			VectorOperations::Copy(src_leaf, dst_leaf, count * flat_size, 0, 0);
		} else {
			// Non-identity permutation: reorder elements from physical to row-major order.
			auto permutation = DecodePermutation(alias);
			auto ndim = permutation.size();

			// Extract logical shape from the result (nested ARRAY) type
			vector<idx_t> shape;
			auto current = result.GetType();
			while (current.id() == LogicalTypeId::ARRAY) {
				shape.push_back(ArrayType::GetSize(current));
				current = ArrayType::GetChildType(current);
			}

			// Compute strides for the physical layout (permuted dimensions)
			// Physical layout: dimension permutation[0] varies slowest, permutation[ndim-1] varies fastest
			vector<idx_t> physical_strides(ndim);
			physical_strides[ndim - 1] = 1;
			for (int i = NumericCast<int>(ndim) - 2; i >= 0; i--) {
				physical_strides[i] = physical_strides[i + 1] * shape[permutation[i + 1]];
			}

			// Compute strides for row-major (logical) layout
			vector<idx_t> logical_strides(ndim);
			logical_strides[ndim - 1] = 1;
			for (int i = NumericCast<int>(ndim) - 2; i >= 0; i--) {
				logical_strides[i] = logical_strides[i + 1] * shape[i + 1];
			}

			// Navigate to leaf data
			Vector *dst = &result;
			while (ArrayType::GetChildType(dst->GetType()).id() == LogicalTypeId::ARRAY) {
				dst = &ArrayVector::GetChildMutable(*dst);
			}
			auto &dst_leaf = ArrayVector::GetChildMutable(*dst);
			auto &src_leaf = ArrayVector::GetChildMutable(source);
			// Source is ARRAY(T, N) — get the byte size of element type T
			auto element_physical_type = ArrayType::GetChildType(source.GetType()).InternalType();
			auto type_size = GetTypeIdSize(element_physical_type);

			// For each tensor in the batch, reorder elements
			src_leaf.Flatten(count * flat_size);
			auto src_ptr = FlatVector::GetData(src_leaf);
			auto dst_ptr = FlatVector::GetDataMutable(dst_leaf);

			for (idx_t row = 0; row < count; row++) {
				auto row_offset = row * flat_size;
				// For each element, compute its logical index from physical position
				for (idx_t phys_pos = 0; phys_pos < flat_size; phys_pos++) {
					// Convert physical position to multi-index using permuted dimension order
					idx_t remaining = phys_pos;
					vector<idx_t> multi_idx(ndim);
					for (idx_t d = 0; d < ndim; d++) {
						auto dim = permutation[d];
						multi_idx[dim] = remaining / physical_strides[d];
						remaining %= physical_strides[d];
					}
					// Convert multi-index to row-major position
					idx_t logical_pos = 0;
					for (idx_t d = 0; d < ndim; d++) {
						logical_pos += multi_idx[d] * logical_strides[d];
					}
					memcpy(dst_ptr + (row_offset + logical_pos) * type_size,
					       src_ptr + (row_offset + phys_pos) * type_size, type_size);
				}
			}
		}

		// Copy validity from source to result
		auto &src_validity = FlatVector::Validity(source);
		auto &dst_validity = FlatVector::ValidityMutable(result);
		for (idx_t i = 0; i < count; i++) {
			if (!src_validity.RowIsValid(i)) {
				dst_validity.SetInvalid(i);
			}
		}
	}

	static void DuckToArrow(ClientContext &context, Vector &source, Vector &result, idx_t count) {
		// source: nested ARRAY(ARRAY(...), ...)
		// result: flat ARRAY(T, product(shape))
		// Navigate to innermost source child
		Vector *src = &source;
		while (ArrayType::GetChildType(src->GetType()).id() == LogicalTypeId::ARRAY) {
			src = &ArrayVector::GetChildMutable(*src);
		}
		auto &src_leaf = ArrayVector::GetChildMutable(*src);

		auto &dst_leaf = ArrayVector::GetChildMutable(result);
		auto flat_size = ArrayType::GetSize(result.GetType());

		VectorOperations::Copy(src_leaf, dst_leaf, count * flat_size, 0, 0);

		auto &src_validity = FlatVector::Validity(source);
		auto &dst_validity = FlatVector::ValidityMutable(result);
		for (idx_t i = 0; i < count; i++) {
			if (!src_validity.RowIsValid(i)) {
				dst_validity.SetInvalid(i);
			}
		}
	}

	static void PopulateSchema(DuckDBArrowSchemaHolder &root_holder, ArrowSchema &schema, const LogicalType &type,
	                           ClientContext &context, const ArrowTypeExtension &extension) {
		// Extract shape from nested ARRAY type
		vector<idx_t> shape;
		idx_t total = 1;
		auto current = type;
		while (current.id() == LogicalTypeId::ARRAY) {
			auto sz = ArrayType::GetSize(current);
			shape.push_back(sz);
			total *= sz;
			current = ArrayType::GetChildType(current);
		}

		// Set format to flat FixedSizeList
		auto format_str = "+w:" + to_string(total);
		auto format = make_unsafe_uniq_array<char>(format_str.size() + 1);
		for (idx_t i = 0; i < format_str.size(); i++) {
			format[i] = format_str[i];
		}
		format[format_str.size()] = '\0';
		root_holder.extension_format.emplace_back(std::move(format));
		schema.format = root_holder.extension_format.back().get();

		// Build shape JSON metadata
		ArrowSchemaMetadata schema_metadata;
		schema_metadata.AddOption(ArrowSchemaMetadata::ARROW_EXTENSION_NAME, "arrow.fixed_shape_tensor");

		string shape_json = "{\"shape\": [";
		for (idx_t i = 0; i < shape.size(); i++) {
			if (i > 0) {
				shape_json += ", ";
			}
			shape_json += to_string(shape[i]);
		}
		shape_json += "]}";
		schema_metadata.AddOption(ArrowSchemaMetadata::ARROW_METADATA_KEY, shape_json);

		root_holder.metadata_info.emplace_back(schema_metadata.SerializeMetadata());
		schema.metadata = root_holder.metadata_info.back().get();
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

	config.RegisterArrowExtension(
	    {"geoarrow.wkb", ArrowGeometry::PopulateSchema, ArrowGeometry::GetType,
	     make_shared_ptr<ArrowTypeExtensionData>(LogicalType::GEOMETRY(), LogicalType::BLOB, ArrowGeometry::ArrowToDuck,
	                                             ArrowGeometry::DuckToArrow)});

	// Types that are 1:n
	config.RegisterArrowExtension({"arrow.json", &ArrowJson::PopulateSchema, &ArrowJson::GetType,
	                               make_shared_ptr<ArrowTypeExtensionData>(LogicalType::JSON())});

	config.RegisterArrowExtension({"DuckDB", "bit", &ArrowBit::PopulateSchema, &ArrowBit::GetType,
	                               make_shared_ptr<ArrowTypeExtensionData>(LogicalType::BIT), nullptr, nullptr});

	config.RegisterArrowExtension({"DuckDB", "bignum", &ArrowBignum::PopulateSchema, &ArrowBignum::GetType,
	                               make_shared_ptr<ArrowTypeExtensionData>(LogicalType::BIGNUM), nullptr, nullptr});

	// FixedShapeTensor: reads flat FixedSizeList with shape metadata as nested ARRAY.
	// Export of multi-dimensional arrays as FixedShapeTensor is handled directly in
	// arrow_converter.cpp (schema) and arrow_duck_schema.cpp (data), gated by
	// arrow_lossless_conversion. This registration only handles the import path.
	config.RegisterArrowExtension({"arrow.fixed_shape_tensor", nullptr, &ArrowFixedShapeTensor::GetType, nullptr});
}
} // namespace duckdb
