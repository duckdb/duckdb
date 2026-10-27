//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/default/default_macro_qualifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/identifier.hpp"

namespace duckdb {
class MacroFunction;

//! Qualifies the references inside a built-in macro body with the system catalog when the
//! default entry is created, so the body binds the same way under any caller search path
class DefaultMacroQualifier {
public:
	//! Qualify every name-resolved reference in the macro's body and default parameter values.
	//! References resolve to the macro's own schema when that schema defines a default macro of
	//! that name, otherwise to the default schema of the system catalog.
	static void Qualify(MacroFunction &macro, const Identifier &macro_schema);
};

} // namespace duckdb
