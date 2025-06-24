//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arrow/arrow_type_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/query_result.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/main/chunk_scan_state.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include <mutex>

namespace duckdb {
class ArrowSchemaMetadata;
struct DuckDBArrowSchemaHolder;

struct DBConfig;
struct ArrowExtensionMetadata {
public:
	ArrowExtensionMetadata() {
	}

	ArrowExtensionMetadata(string extension_name, string vendor_name, string type_name, string arrow_format);

	hash_t GetHash() const;

	string ToString() const;

	string GetExtensionName() const;

	string GetVendorName() const;

	string GetTypeName() const;

	string GetArrowFormat() const;

	void SetArrowFormat(string arrow_format);

	bool IsCanonical() const;

	bool operator==(const ArrowExtensionMetadata &other) const;

	//! Arrow Extension for non-canonical types.
	static constexpr const char *ARROW_EXTENSION_NON_CANONICAL = "arrow.opaque";

private:
	//! The extension name (e.g., 'arrow.uuid', 'arrow.opaque',...)
	string extension_name {};
	//! If the extension name is 'arrow.opaque' a vendor and type must be defined.
	//! The vendor_name is the system that produced the type (e.g., DuckDB)
	string vendor_name {};
	//! The type_name is the name of the type produced by the vendor (e.g., hugeint)
	string type_name {};
	//! The arrow format (e.g., z)
	string arrow_format {};
};

class ArrowTypeExtension;

typedef void (*populate_arrow_schema_t)(DuckDBArrowSchemaHolder &root_holder, ArrowSchema &child,
                                        const LogicalType &type, ClientContext &context,
                                        const ArrowTypeExtension &extension);

typedef unique_ptr<ArrowType> (*get_type_t)(const ArrowSchema &schema, const ArrowSchemaMetadata &schema_metadata);

class ArrowTypeExtension {
public:
	ArrowTypeExtension() {};
	//! This type is not registered, so we just use whatever is the format and hope for the best
	explicit ArrowTypeExtension(ArrowExtensionMetadata &extension_metadata, unique_ptr<ArrowType> type);
	//! We either have simple extensions where we only return one type
	ArrowTypeExtension(string extension_name, string arrow_format, shared_ptr<ArrowTypeExtensionData> type);
	ArrowTypeExtension(string vendor_name, string type_name, string arrow_format,
	                   shared_ptr<ArrowTypeExtensionData> type);

	//! We have complex extensions, where we can return multiple types, hence we must have callback functions to do so
	ArrowTypeExtension(string extension_name, populate_arrow_schema_t populate_arrow_schema, get_type_t get_type,
	                   shared_ptr<ArrowTypeExtensionData> type);
	ArrowTypeExtension(string vendor_name, string type_name, populate_arrow_schema_t populate_arrow_schema,
	                   get_type_t get_type, shared_ptr<ArrowTypeExtensionData> type, cast_arrow_duck_t arrow_to_duckdb,
	                   cast_duck_arrow_t duckdb_to_arrow);

	ArrowExtensionMetadata GetInfo() const;

	unique_ptr<ArrowType> GetType(const ArrowSchema &schema, const ArrowSchemaMetadata &schema_metadata) const;

	shared_ptr<ArrowTypeExtensionData> GetTypeExtension() const;

	LogicalTypeId GetLogicalTypeId() const;

	LogicalType GetLogicalType() const;

	bool HasType() const;

	static void PopulateArrowSchema(DuckDBArrowSchemaHolder &root_holder, ArrowSchema &child,
	                                const LogicalType &duckdb_type, ClientContext &context,
	                                const ArrowTypeExtension &extension);

	//! (Optional) Callback to a function that sets up the arrow schema production
	populate_arrow_schema_t populate_arrow_schema = nullptr;
	//! (Optional) Callback to a function that sets up the arrow schema production
	get_type_t get_type = nullptr;

private:
	//! Extension Info from Arrow
	ArrowExtensionMetadata extension_metadata;
	//! Arrow Extension Type
	shared_ptr<ArrowTypeExtensionData> type_extension;
};

struct HashArrowTypeExtension {
	size_t operator()(ArrowExtensionMetadata const &arrow_extension_info) const noexcept {
		return arrow_extension_info.GetHash();
	}
};

struct TypeInfo {
	TypeInfo();
	explicit TypeInfo(const LogicalType &type);
	explicit TypeInfo(string alias);
	string alias;
	LogicalTypeId type;
	hash_t GetHash() const;
	bool operator==(const TypeInfo &other) const;
};

struct HashTypeInfo {
	size_t operator()(TypeInfo const &type_info) const noexcept {
		return type_info.GetHash();
	}
};

//! The set of encoding functions
struct ArrowTypeExtensionSet {
	ArrowTypeExtensionSet() {};
	static void Initialize(const DBConfig &config);
	std::mutex lock;
	unordered_map<ArrowExtensionMetadata, ArrowTypeExtension, HashArrowTypeExtension> type_extensions;
	unordered_map<TypeInfo, vector<ArrowExtensionMetadata>, HashTypeInfo> type_to_info;
};

} // namespace duckdb
