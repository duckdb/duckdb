#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/constraints/bound_check_constraint.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"

#include <sstream>

namespace duckdb {

TableCatalogEntry::TableCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : StandardEntry(CatalogType::TABLE_ENTRY, schema, catalog, info.table), columns(std::move(info.columns)),
      constraints(std::move(info.constraints)) {
	this->temporary = info.temporary;
	this->dependencies = info.dependencies;
	this->comment = info.comment;
	this->tags = info.tags;
}

bool TableCatalogEntry::HasGeneratedColumns() const {
	return columns.LogicalColumnCount() != columns.PhysicalColumnCount();
}

LogicalIndex TableCatalogEntry::GetColumnIndex(string &column_name, bool if_exists) const {
	auto entry = columns.GetColumnIndex(column_name);
	if (!entry.IsValid()) {
		if (if_exists) {
			return entry;
		}
		throw BinderException("Table \"%s\" does not have a column with name \"%s\"", name, column_name);
	}
	return entry;
}

unique_ptr<BlockingSample> TableCatalogEntry::GetSample() {
	return nullptr;
}

bool TableCatalogEntry::ColumnExists(const string &name) const {
	return columns.ColumnExists(name);
}

const ColumnDefinition &TableCatalogEntry::GetColumn(const string &name) const {
	return columns.GetColumn(name);
}

vector<LogicalType> TableCatalogEntry::GetTypes() const {
	vector<LogicalType> types;
	for (auto &col : columns.Physical()) {
		types.push_back(col.Type());
	}
	return types;
}

unique_ptr<CreateInfo> TableCatalogEntry::GetInfo() const {
	auto result = make_uniq<CreateTableInfo>();
	result->catalog = catalog.GetName();
	result->schema = schema.name;
	result->table = name;
	result->columns = columns.Copy();
	result->constraints.reserve(constraints.size());
	result->dependencies = dependencies;
	std::for_each(constraints.begin(), constraints.end(),
	              [&result](const unique_ptr<Constraint> &c) { result->constraints.emplace_back(c->Copy()); });
	result->comment = comment;
	result->tags = tags;
	return std::move(result);
}

string TableCatalogEntry::ColumnsToSQL(const ColumnList &columns, const vector<unique_ptr<Constraint>> &constraints) {
	std::stringstream ss;

	ss << "(";

	// find all columns that have NOT NULL specified, but are NOT primary key columns
	logical_index_set_t not_null_columns;
	logical_index_set_t unique_columns;
	logical_index_set_t pk_columns;
	unordered_set<string> multi_key_pks;
	vector<string> extra_constraints;
	for (auto &constraint : constraints) {
		if (constraint->type == ConstraintType::NOT_NULL) {
			auto &not_null = constraint->Cast<NotNullConstraint>();
			not_null_columns.insert(not_null.index);
		} else if (constraint->type == ConstraintType::UNIQUE) {
			auto &pk = constraint->Cast<UniqueConstraint>();
			if (pk.HasIndex()) {
				// no columns specified: single column constraint
				if (pk.IsPrimaryKey()) {
					pk_columns.insert(pk.GetIndex());
				} else {
					unique_columns.insert(pk.GetIndex());
				}
			} else {
				// multi-column constraint, this constraint needs to go at the end after all columns
				if (pk.IsPrimaryKey()) {
					// multi key pk column: insert set of columns into multi_key_pks
					for (auto &col : pk.GetColumnNames()) {
						multi_key_pks.insert(col);
					}
				}
				extra_constraints.push_back(constraint->ToString());
			}
		} else if (constraint->type == ConstraintType::FOREIGN_KEY) {
			auto &fk = constraint->Cast<ForeignKeyConstraint>();
			if (fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE ||
			    fk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				extra_constraints.push_back(constraint->ToString());
			}
		} else {
			extra_constraints.push_back(constraint->ToString());
		}
	}

	for (auto &column : columns.Logical()) {
		if (column.Oid() > 0) {
			ss << ", ";
		}
		ss << KeywordHelper::WriteOptionallyQuoted(column.Name()) << " ";
		auto &column_type = column.Type();
		if (column_type.id() != LogicalTypeId::ANY) {
			ss << column.Type().ToString();
		}
		auto extra_type_info = column_type.AuxInfo();
		if (extra_type_info && extra_type_info->type == ExtraTypeInfoType::STRING_TYPE_INFO) {
			auto &string_info = extra_type_info->Cast<StringTypeInfo>();
			if (!string_info.collation.empty()) {
				ss << " COLLATE " + string_info.collation;
			}
		}
		bool not_null = not_null_columns.find(column.Logical()) != not_null_columns.end();
		bool is_single_key_pk = pk_columns.find(column.Logical()) != pk_columns.end();
		bool is_multi_key_pk = multi_key_pks.find(column.Name()) != multi_key_pks.end();
		bool is_unique = unique_columns.find(column.Logical()) != unique_columns.end();
		if (column.Generated()) {
			reference<const ParsedExpression> generated_expression = column.GeneratedExpression();
			if (column_type.id() != LogicalTypeId::ANY) {
				// We artificially add a cast if the type is specified, need to strip it
				auto &expr = generated_expression.get();
				D_ASSERT(expr.GetExpressionType() == ExpressionType::OPERATOR_CAST);
				auto &cast_expr = expr.Cast<CastExpression>();
				D_ASSERT(cast_expr.cast_type.id() == column_type.id());
				generated_expression = *cast_expr.child;
			}
			ss << " GENERATED ALWAYS AS(" << generated_expression.get().ToString() << ")";
		} else if (column.HasDefaultValue()) {
			ss << " DEFAULT(" << column.DefaultValue().ToString() << ")";
		}
		if (not_null && !is_single_key_pk && !is_multi_key_pk) {
			// NOT NULL but not a primary key column
			ss << " NOT NULL";
		}
		if (is_single_key_pk) {
			// single column pk: insert constraint here
			ss << " PRIMARY KEY";
		}
		if (is_unique) {
			// single column unique: insert constraint here
			ss << " UNIQUE";
		}
	}
	// print any extra constraints that still need to be printed
	for (auto &extra_constraint : extra_constraints) {
		ss << ", ";
		ss << extra_constraint;
	}

	ss << ")";
	return ss.str();
}

string TableCatalogEntry::ColumnNamesToSQL(const ColumnList &columns) {
	if (columns.empty()) {
		return "";
	}

	std::stringstream ss;
	ss << "(";

	for (auto &column : columns.Logical()) {
		if (column.Oid() > 0) {
			ss << ", ";
		}
		ss << KeywordHelper::WriteOptionallyQuoted(column.Name()) << " ";
	}
	ss << ")";
	return ss.str();
}

string TableCatalogEntry::ToSQL() const {
	auto create_info = GetInfo();
	return create_info->ToString();
}

const ColumnList &TableCatalogEntry::GetColumns() const {
	return columns;
}

const ColumnDefinition &TableCatalogEntry::GetColumn(LogicalIndex idx) const {
	return columns.GetColumn(idx);
}

const vector<unique_ptr<Constraint>> &TableCatalogEntry::GetConstraints() const {
	return constraints;
}

// LCOV_EXCL_START
DataTable &TableCatalogEntry::GetStorage() {
	throw InternalException("Calling GetStorage on a TableCatalogEntry that is not a DuckTableEntry");
}
// LCOV_EXCL_STOP

static void BindExtraColumns(TableCatalogEntry &table, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
                             physical_index_set_t &bound_columns) {
	if (bound_columns.size() <= 1) {
		return;
	}
	idx_t found_column_count = 0;
	physical_index_set_t found_columns;
	for (idx_t i = 0; i < update.columns.size(); i++) {
		if (bound_columns.find(update.columns[i]) != bound_columns.end()) {
			// this column is referenced in the CHECK constraint
			found_column_count++;
			found_columns.insert(update.columns[i]);
		}
	}
	if (found_column_count > 0 && found_column_count != bound_columns.size()) {
		// columns in this CHECK constraint were referenced, but not all were part of the UPDATE
		// add them to the scan and update set
		for (auto &check_column_id : bound_columns) {
			if (found_columns.find(check_column_id) != found_columns.end()) {
				// column is already projected
				continue;
			}
			// column is not projected yet: project it by adding the clause "i=i" to the set of updated columns
			auto &column = table.GetColumns().GetColumn(check_column_id);
			update.expressions.push_back(make_uniq<BoundColumnRefExpression>(
			    column.Type(), ColumnBinding(proj.table_index, proj.expressions.size())));
			proj.expressions.push_back(make_uniq<BoundColumnRefExpression>(
			    column.Type(), ColumnBinding(get.table_index, get.GetColumnIds().size())));
			get.AddColumnId(check_column_id.index);
			update.columns.push_back(check_column_id);
		}
	}
}

vector<ColumnSegmentInfo> TableCatalogEntry::GetColumnSegmentInfo() {
	return {};
}

void TableCatalogEntry::BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj,
                                              LogicalUpdate &update, ClientContext &context) {
	// check the constraints and indexes of the table to see if we need to project any additional columns
	// we do this for indexes with multiple columns and CHECK constraints in the UPDATE clause
	// suppose we have a constraint CHECK(i + j < 10); now we need both i and j to check the constraint
	// if we are only updating one of the two columns we add the other one to the UPDATE set
	// with a "useless" update (i.e. i=i) so we can verify that the CHECK constraint is not violated
	auto bound_constraints = binder.BindConstraints(constraints, name, columns);
	for (auto &constraint : bound_constraints) {
		if (constraint->type == ConstraintType::CHECK) {
			auto &check = constraint->Cast<BoundCheckConstraint>();
			// check constraint! check if we need to add any extra columns to the UPDATE clause
			BindExtraColumns(*this, get, proj, update, check.bound_columns);
		}
	}
	if (update.return_chunk) {
		physical_index_set_t all_columns;
		for (auto &column : GetColumns().Physical()) {
			all_columns.insert(column.Physical());
		}
		BindExtraColumns(*this, get, proj, update, all_columns);
	}
	// for index updates we always turn any update into an insert and a delete
	// we thus need all the columns to be available, hence we check if the update touches any index columns
	// If the returning keyword is used, we need access to the whole row in case the user requests it.
	// Therefore switch the update to a delete and insert.
	update.update_is_del_and_insert = false;
	TableStorageInfo table_storage_info = GetStorageInfo(context);
	for (auto index : table_storage_info.index_info) {
		for (auto &column : update.columns) {
			if (index.column_set.find(column.index) != index.column_set.end()) {
				update.update_is_del_and_insert = true;
				break;
			}
		}
	};

	// we also convert any updates on LIST columns into delete + insert
	for (auto &col_index : update.columns) {
		auto &column = GetColumns().GetColumn(col_index);
		if (!column.Type().SupportsRegularUpdate()) {
			update.update_is_del_and_insert = true;
			break;
		}
	}

	if (update.update_is_del_and_insert) {
		// the update updates a column required by an index or requires returning the updated rows,
		// push projections for all columns
		physical_index_set_t all_columns;
		for (auto &column : GetColumns().Physical()) {
			all_columns.insert(column.Physical());
		}
		BindExtraColumns(*this, get, proj, update, all_columns);
	}
}

optional_ptr<Constraint> TableCatalogEntry::GetPrimaryKey() const {
	for (const auto &constraint : GetConstraints()) {
		if (constraint->type == ConstraintType::UNIQUE) {
			auto &unique = constraint->Cast<UniqueConstraint>();
			if (unique.IsPrimaryKey()) {
				return &unique;
			}
		}
	}
	return nullptr;
}

bool TableCatalogEntry::HasPrimaryKey() const {
	return GetPrimaryKey() != nullptr;
}

} // namespace duckdb
