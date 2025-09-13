//===----------------------------------------------------------------------===//
//                         DuckDB
//
// autocomplete_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

class AutocompleteExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
	std::string Version() const override;
	static void RegisterKeywords(ExtensionLoader &loader);
};

} // namespace duckdb
