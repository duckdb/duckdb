//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arrow/arrow_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/query_result.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/main/chunk_scan_state.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"

namespace duckdb {

struct DBConfig;
struct ArrowExtensionInfo {
public:
	ArrowExtensionInfo() {
	}
	ArrowExtensionInfo(string extension_name, string arrow_format);
	ArrowExtensionInfo(string vendor_name, string type_name, string arrow_format);
	ArrowExtensionInfo(string extension_name, string vendor_name, string type_name, string arrow_format);

	hash_t GetHash() const;

	string ToString() const;

	string GetExtensionName() const;

	string GetVendorName() const;

	string GetTypeName() const;

	string GetArrowFormat() const;

	bool IsCanonical() const;

	bool operator==(const ArrowExtensionInfo &other) const;

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

class ArrowExtension {
public:
	ArrowExtension() {};
	ArrowExtension(string extension_name, string arrow_format, shared_ptr<ArrowType> type);
	ArrowExtension(string vendor_name, string type_name, string arrow_format, shared_ptr<ArrowType> type);

	ArrowExtensionInfo GetInfo() const;

	shared_ptr<ArrowType> GetType() const;

	LogicalTypeId GetLogicalTypeId() const;

private:
	//! Extension Info from Arrow
	ArrowExtensionInfo extension_info;
	//! Arrow Type
	shared_ptr<ArrowType> type;
};

struct HashArrowExtension {
	size_t operator()(ArrowExtensionInfo const &arrow_extension_info) const noexcept {
		return arrow_extension_info.GetHash();
	}
};

//! The set of encoding functions
struct ArrowExtensionSet {
	ArrowExtensionSet() {};
	static void Initialize(DBConfig &config);
	std::mutex lock;
	unordered_map<ArrowExtensionInfo, ArrowExtension, HashArrowExtension> extensions;

	unordered_map<LogicalTypeId, vector<ArrowExtensionInfo>> type_to_info;
};

} // namespace duckdb
