//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arrow/schema_metadata.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/query_result.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/main/chunk_scan_state.hpp"
#include "duckdb/common/arrow/arrow_type_extension.hpp"
#include "duckdb/common/complex_json.hpp"

namespace duckdb {
class ArrowSchemaMetadata {
public:
	//! Constructor used to read a metadata schema, used when importing an arrow object
	explicit ArrowSchemaMetadata(const char *metadata);
	//! Constructor used to create a metadata schema, used when exporting an arrow object
	ArrowSchemaMetadata();
	//! Adds an option to the metadata
	void AddOption(const string &key, const string &value);
	//! Gets an option from the metadata, returns an empty string if it does not exist.
	string GetOption(const string &key) const;
	//! Transforms metadata to a char*, used when creating an arrow object
	unsafe_unique_array<char> SerializeMetadata() const;
	//! If the arrow extension is set
	bool HasExtension() const;

	ArrowExtensionMetadata GetExtensionInfo(string format);
	//! Get the extension name if set, otherwise returns empty
	string GetExtensionName() const;
	//! Key for encode of the extension type name
	static constexpr const char *ARROW_EXTENSION_NAME = "ARROW:extension:name";
	//! Key for encode of the metadata key
	static constexpr const char *ARROW_METADATA_KEY = "ARROW:extension:metadata";
	//! Creates the metadata based on an extension name
	static ArrowSchemaMetadata ArrowCanonicalType(const string &extension_name);
	//! Creates the metadata based on an extension name
	static ArrowSchemaMetadata NonCanonicalType(const string &type_name, const string &vendor_name);

private:
	//! The unordered map that holds the metadata
	unordered_map<string, string> schema_metadata_map;
	//! The extension metadata map, currently only used for internal types in arrow.opaque
	unique_ptr<ComplexJSON> extension_metadata_map;
};
} // namespace duckdb
