//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/external_resources_mode.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! How much of the external resource feature is available. Ordered by increasing restriction: the mode
//! degrades one way only, so a new mode is accepted only when it compares greater than the current one.
enum class ExternalResourcesMode : uint8_t {
	//! Everything: provisioning, teardown, type registration and listing.
	AVAILABLE = 0,
	//! Read-only: listing and discovery, but no create, destroy or type registration.
	LISTING = 1,
	//! Nothing: every external resource operation is rejected.
	OFF = 2
};

} // namespace duckdb
