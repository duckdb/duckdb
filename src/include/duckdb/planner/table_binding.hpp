//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/table_binding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/catalog/catalog_entry/table_column_type.hpp"
#include "duckdb/planner/binding_alias.hpp"
#include "duckdb/common/column_index.hpp"

namespace duckdb {
class BindContext;
class BoundQueryNode;
class ColumnRefExpression;
class SubqueryRef;
class LogicalGet;
class TableCatalogEntry;
class TableFunctionCatalogEntry;
class BoundTableFunction;
class StandardEntry;
struct ColumnBinding;

enum class BindingType { BASE, TABLE, DUMMY, CATALOG_ENTRY };

//! A Binding represents a binding to a table, table-producing function or subquery with a specified table index.
struct Binding {
	Binding(BindingType binding_type, BindingAlias alias, vector<LogicalType> types, vector<string> names, idx_t index);
	virtual ~Binding() = default;

	//! The type of Binding
	BindingType binding_type;
	//! The alias of the binding
	BindingAlias alias;
	//! The table index of the binding
	idx_t index;
	//! The types of the bound columns
	vector<LogicalType> types;
	//! Column names of the subquery
	vector<string> names;
	//! Name -> index for the names
	case_insensitive_map_t<column_t> name_map;

public:
	bool TryGetBindingIndex(const string &column_name, column_t &column_index);
	column_t GetBindingIndex(const string &column_name);
	bool HasMatchingBinding(const string &column_name);
	virtual ErrorData ColumnNotFoundError(const string &column_name) const;
	virtual BindResult Bind(ColumnRefExpression &colref, idx_t depth);
	virtual optional_ptr<StandardEntry> GetStandardEntry();
	string GetAlias() const;

	static BindingAlias GetAlias(const string &explicit_alias, const StandardEntry &entry);
	static BindingAlias GetAlias(const string &explicit_alias, optional_ptr<StandardEntry> entry);

public:
	template <class TARGET>
	TARGET &Cast() {
		if (binding_type != TARGET::TYPE) {
			throw InternalException("Failed to cast binding to type - binding type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (binding_type != TARGET::TYPE) {
			throw InternalException("Failed to cast binding to type - binding type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct EntryBinding : public Binding {
public:
	static constexpr const BindingType TYPE = BindingType::CATALOG_ENTRY;

public:
	EntryBinding(const string &alias, vector<LogicalType> types, vector<string> names, idx_t index,
	             StandardEntry &entry);
	StandardEntry &entry;

public:
	optional_ptr<StandardEntry> GetStandardEntry() override;
};

//! TableBinding is exactly like the Binding, except it keeps track of which columns were bound in the linked LogicalGet
//! node for projection pushdown purposes.
struct TableBinding : public Binding {
public:
	static constexpr const BindingType TYPE = BindingType::TABLE;

public:
	TableBinding(const string &alias, vector<LogicalType> types, vector<string> names,
	             vector<ColumnIndex> &bound_column_ids, optional_ptr<StandardEntry> entry, idx_t index,
	             bool add_row_id = false);

	//! A reference to the set of bound column ids
	vector<ColumnIndex> &bound_column_ids;
	//! The underlying catalog entry (if any)
	optional_ptr<StandardEntry> entry;

public:
	unique_ptr<ParsedExpression> ExpandGeneratedColumn(const string &column_name);
	BindResult Bind(ColumnRefExpression &colref, idx_t depth) override;
	optional_ptr<StandardEntry> GetStandardEntry() override;
	ErrorData ColumnNotFoundError(const string &column_name) const override;
	// These are columns that are present in the name_map, appearing in the order that they're bound
	const vector<ColumnIndex> &GetBoundColumnIds() const;

protected:
	ColumnBinding GetColumnBinding(column_t column_index);
};

//! DummyBinding is like the Binding, except the alias and index are set by default.
//! Used for binding lambdas and macro parameters.
struct DummyBinding : public Binding {
public:
	static constexpr const BindingType TYPE = BindingType::DUMMY;
	// NOTE: changing this string conflicts with the storage version
	static constexpr const char *DUMMY_NAME = "0_macro_parameters";

public:
	DummyBinding(vector<LogicalType> types, vector<string> names, string dummy_name);

	//! Arguments (for macros)
	vector<unique_ptr<ParsedExpression>> *arguments;
	//! The name of the dummy binding
	string dummy_name;

public:
	//! Binding macros
	BindResult Bind(ColumnRefExpression &col_ref, idx_t depth) override;
	//! Binding lambdas
	BindResult Bind(LambdaRefExpression &lambda_ref, idx_t depth);

	//! Returns a copy of the col_ref parameter as a parsed expression
	unique_ptr<ParsedExpression> ParamToArg(ColumnRefExpression &col_ref);
};

} // namespace duckdb
