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

namespace duckdb {
class ArrowExtension {
public:
	ArrowExtension(string extension_name, string arrow_format);
	ArrowExtension(string vendor_name, string type_name, string arrow_format);
	unique_ptr<ArrowType> GetArrowExtensionType();
	//! Arrow Extension for non-canonical types.
	static constexpr const char *ARROW_EXTENSION_NON_CANONICAL = "arrow.opaque";

private:
	//! The extension name (e.g., 'arrow.uuid', 'arrow.opaque',...)
	string extension_name;
	//! If the extension name is 'arrow.opaque' a vendor and type must be defined.
	//! The vendor_name is the system that produced the type (e.g., DuckDB)
	string vendor_name;
	//! The type_name is the name of the type produced by the vendor (e.g., hugeint)
	string type_name;
	//! The arrow format (e.g., z)
	string arrow_format;
};
} // namespace duckdb
