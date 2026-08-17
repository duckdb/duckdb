//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/async_io_callback.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/optional_ptr.hpp"

#include <functional>

namespace duckdb {

//! Invoked exactly once when an asynchronous I/O operation completes, [error] is null when it succeeded.
//! May be invoked on any thread, including inline from the call that started the operation.
using AsyncIOCallback = std::function<void(optional_ptr<ErrorData> error)>;

} // namespace duckdb
