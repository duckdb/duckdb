//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/macro_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdint.h>
#include <string>
#include <utility>

#include "duckdb/function/function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/table_binding.hpp"

namespace duckdb {

class ScalarMacroFunction;
class Binder;
class Deserializer;
class FunctionExpression;
class Serializer;

enum class MacroType : uint8_t { VOID_MACRO = 0, TABLE_MACRO = 1, SCALAR_MACRO = 2 };

struct MacroBindResult {
	explicit MacroBindResult(string error_p) : error(std::move(error_p)) {
	}
	explicit MacroBindResult(idx_t function_idx) : function_idx(function_idx) {
	}

	optional_idx function_idx;
	string error;
};

class MacroFunction {
public:
	explicit MacroFunction(MacroType type);

	//! The type
	MacroType type;
	//! The parameters (ColumnRefExpression)
	vector<unique_ptr<ParsedExpression>> parameters;
	//! The default values of the parameters
	InsertionOrderPreservingMap<unique_ptr<ParsedExpression>> default_parameters;
	//! The types of the parameters
	vector<LogicalType> types;

public:
	virtual ~MacroFunction() {
	}

	void CopyProperties(MacroFunction &other) const;

	virtual unique_ptr<MacroFunction> Copy() const = 0;

	vector<unique_ptr<ParsedExpression>> GetPositionalParametersForSerialization(Serializer &serializer) const;
	void FinalizeDeserialization();

	static MacroBindResult BindMacroFunction(Binder &binder, const vector<unique_ptr<MacroFunction>> &macro_functions,
	                                         const string &name, FunctionExpression &function_expr,
	                                         vector<unique_ptr<ParsedExpression>> &positional_arguments,
	                                         InsertionOrderPreservingMap<unique_ptr<ParsedExpression>> &named_arguments,
	                                         idx_t depth);
	static unique_ptr<DummyBinding>
	CreateDummyBinding(const MacroFunction &macro_def, const string &name,
	                   vector<unique_ptr<ParsedExpression>> &positional_arguments,
	                   InsertionOrderPreservingMap<unique_ptr<ParsedExpression>> &named_arguments);

	virtual string ToSQL() const;

	virtual void Serialize(Serializer &serializer) const;
	static unique_ptr<MacroFunction> Deserialize(Deserializer &deserializer);

public:
	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast macro to type - macro type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast macro to type - macro type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}
};

} // namespace duckdb
