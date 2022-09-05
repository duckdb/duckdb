//===----------------------------------------------------------------------===//
//                         DuckDB
//
// test/include/test_helper_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/extension.hpp"

namespace duckdb {

class TestHelperExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
	//! Report the last error message.
	static void SetLastError(const string &error);
	//! Clear the last error message previously reported.
	static void ClearLastError();

	static unique_ptr<string> last_error;
};

} // namespace duckdb
