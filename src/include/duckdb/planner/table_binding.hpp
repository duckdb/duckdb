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
	Binding(BindingType binding_type, const string &alias, vector<LogicalType> types, vector<string> names,
	        idx_t index);
	virtual ~Binding() = default;

	//! The type of Binding
	BindingType binding_type;
	//! The alias of the binding
	string alias;
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
	virtual string ColumnNotFoundError(const string &column_name) const;
	virtual BindResult Bind(ColumnRefExpression &colref, idx_t depth);
	virtual optional_ptr<StandardEntry> GetStandardEntry();

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
	             vector<column_t> &bound_column_ids, optional_ptr<StandardEntry> entry, idx_t index,
	             bool add_row_id = false);

	//! A reference to the set of bound column ids
	vector<column_t> &bound_column_ids;
	//! The underlying catalog entry (if any)
	optional_ptr<StandardEntry> entry;

public:
	unique_ptr<ParsedExpression> ExpandGeneratedColumn(const string &column_name);
	BindResult Bind(ColumnRefExpression &colref, idx_t depth) override;
	optional_ptr<StandardEntry> GetStandardEntry() override;
	string ColumnNotFoundError(const string &column_name) const override;
	// These are columns that are present in the name_map, appearing in the order that they're bound
	const vector<column_t> &GetBoundColumnIds() const;

protected:
	ColumnBinding GetColumnBinding(column_t column_index);
};

//! DummyBinding is like the Binding, except the alias and index are set by default. Used for binding lambdas and macro
//! parameters.
struct DummyBinding : public Binding {
public:
	static constexpr const BindingType TYPE = BindingType::DUMMY;
	// NOTE: changing this string conflicts with the storage version
	static constexpr const char *DUMMY_NAME = "0_macro_parameters";

public:
	DummyBinding(vector<LogicalType> types_p, vector<string> names_p, string dummy_name_p);

	//! Arguments
	vector<unique_ptr<ParsedExpression>> *arguments;
	//! The name of the dummy binding
	string dummy_name;

public:
	BindResult Bind(ColumnRefExpression &colref, idx_t depth) override;
	BindResult Bind(ColumnRefExpression &colref, idx_t lambda_index, idx_t depth);

	//! Given the parameter colref, returns a copy of the argument that was supplied for this parameter
	unique_ptr<ParsedExpression> ParamToArg(ColumnRefExpression &colref);
};

} // namespace duckdb
