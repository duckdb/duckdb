#include "duckdb/function/table/system_catalog_pruning.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/profiler/profiling_node.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

bool SystemCatalogPruningData::IsIdentityColumn(idx_t primary_index) const {
	if (database_name_idx.IsValid() && database_name_idx.GetIndex() == primary_index) {
		return true;
	}
	if (database_oid_idx.IsValid() && database_oid_idx.GetIndex() == primary_index) {
		return true;
	}
	if (schema_name_idx.IsValid() && schema_name_idx.GetIndex() == primary_index) {
		return true;
	}
	if (schema_oid_idx.IsValid() && schema_oid_idx.GetIndex() == primary_index) {
		return true;
	}
	return false;
}

unique_ptr<FunctionData> SystemCatalogPruningData::Copy() const {
	auto result = make_uniq<SystemCatalogPruningData>();
	result->database_name_idx = database_name_idx;
	result->database_oid_idx = database_oid_idx;
	result->schema_name_idx = schema_name_idx;
	result->schema_oid_idx = schema_oid_idx;
	for (auto &predicate : predicates) {
		result->predicates.push_back(predicate->Copy());
	}
	return std::move(result);
}

bool SystemCatalogPruningData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<SystemCatalogPruningData>();
	if (database_name_idx != other.database_name_idx || database_oid_idx != other.database_oid_idx ||
	    schema_name_idx != other.schema_name_idx || schema_oid_idx != other.schema_oid_idx) {
		return false;
	}
	if (predicates.size() != other.predicates.size()) {
		return false;
	}
	for (idx_t i = 0; i < predicates.size(); i++) {
		if (!predicates[i]->Equals(*other.predicates[i])) {
			return false;
		}
	}
	return true;
}

unique_ptr<FunctionData> SystemCatalogPruning::CreateBindData(const vector<Identifier> &names) {
	auto result = make_uniq<SystemCatalogPruningData>();
	for (idx_t i = 0; i < names.size(); i++) {
		if (names[i] == "database_name") {
			result->database_name_idx = i;
		} else if (names[i] == "database_oid") {
			result->database_oid_idx = i;
		} else if (names[i] == "schema_name") {
			result->schema_name_idx = i;
		} else if (names[i] == "schema_oid") {
			result->schema_oid_idx = i;
		}
	}
	return std::move(result);
}

void SystemCatalogPruning::PushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data,
                                                 vector<unique_ptr<Expression>> &filters) {
	(void)context; // unused: fixed pushdown_complex_filter signature
	if (!bind_data) {
		return;
	}
	auto &data = bind_data->Cast<SystemCatalogPruningData>();
	auto &column_ids = get.GetColumnIds();

	for (auto &filter : filters) {
		// Skip parameterised predicates: the per-execution rebind can leave the callback a stale placeholder.
		if (filter->IsVolatile() || filter->HasParameter()) {
			continue;
		}
		bool eligible = true;
		auto copy = filter->Copy();
		ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(copy, [&](BoundColumnRefExpression &ref,
		                                                                               unique_ptr<Expression> &) {
			if (!eligible) {
				return;
			}
			auto &binding = ref.Binding();
			if (ref.Depth() != 0 || binding.table_index != get.table_index) {
				eligible = false; // correlated ref or a column from another table
				return;
			}
			auto pos = binding.column_index.GetIndex();
			if (pos >= column_ids.size() || !column_ids[pos].HasPrimaryIndex() || column_ids[pos].IsVirtualColumn()) {
				eligible = false;
				return;
			}
			auto primary = column_ids[pos].GetPrimaryIndex();
			if (!data.IsIdentityColumn(primary)) {
				eligible = false;
				return;
			}
			ref.BindingMutable() = ColumnBinding(get.table_index, ProjectionIndex(primary));
		});
		if (eligible) {
			data.predicates.push_back(std::move(copy));
		}
	}
	// `filters` is left in the plan: the retained predicate above the scan is the correctness gate, so pruning
	// below can only ever skip non-matching candidates.
}

// Substitutes the candidate's known identity-column values into each predicate and folds it. Returns true if any
// predicate folds to false/NULL (the candidate cannot produce a row). A predicate still referencing an unknown
// column is skipped as undecidable at this level.
static bool Rejects(ClientContext &context, const vector<unique_ptr<Expression>> &predicates,
                    const unordered_map<idx_t, Value> &known) {
	for (auto &predicate : predicates) {
		auto copy = predicate->Copy();
		ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
		    copy, [&](BoundColumnRefExpression &ref, unique_ptr<Expression> &expr) {
			    auto it = known.find(ref.Binding().column_index.GetIndex());
			    if (it != known.end()) {
				    expr = make_uniq<BoundConstantExpression>(it->second);
			    }
		    });
		if (!copy->IsScalar() || !copy->IsFoldable()) {
			continue;
		}
		Value result;
		// On a non-fold error TryEvaluateScalar returns false (InternalException still propagates); keep the
		// candidate and let the post-scan filter decide.
		if (!ExpressionExecutor::TryEvaluateScalar(context, *copy, result)) {
			continue;
		}
		if (result.IsNull() || !result.GetValue<bool>()) {
			return true;
		}
	}
	return false;
}

// Matches  schema_name = <foldable VARCHAR>  (bare column either side) and returns the constant in `name`.
// Does not match lower()/upper() wrappers, IN, collated comparisons or COMPARE_NOT_DISTINCT_FROM: those are not
// completeness-safe for a point lookup, so they fall through to list-and-prune.
static bool TryGetSchemaNameEquality(ClientContext &context, const SystemCatalogPruningData &data, string &name) {
	if (!data.schema_name_idx.IsValid()) {
		return false;
	}
	auto schema_name_pos = data.schema_name_idx.GetIndex();
	for (auto &predicate : data.predicates) {
		if (predicate->GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
			continue;
		}
		auto &cmp = predicate->Cast<BoundFunctionExpression>();
		auto &left = *cmp.GetChildren()[0];
		auto &right = *cmp.GetChildren()[1];
		for (idx_t side = 0; side < 2; side++) {
			auto &column_side = side == 0 ? left : right;
			auto &value_side = side == 0 ? right : left;
			if (column_side.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				continue;
			}
			if (column_side.Cast<BoundColumnRefExpression>().Binding().column_index.GetIndex() != schema_name_pos) {
				continue;
			}
			if (!value_side.IsScalar() || !value_side.IsFoldable()) {
				continue;
			}
			Value v;
			if (!ExpressionExecutor::TryEvaluateScalar(context, value_side, v) || v.IsNull() ||
			    v.type().id() != LogicalTypeId::VARCHAR) {
				continue;
			}
			name = StringValue::Get(v);
			return true;
		}
	}
	return false;
}

vector<reference<SchemaCatalogEntry>> SystemCatalogPruning::GetSchemas(ClientContext &context,
                                                                       optional_ptr<const FunctionData> bind_data,
                                                                       SystemCatalogPruningStats &stats) {
	if (!bind_data) {
		return Catalog::GetAllSchemas(context);
	}
	auto &data = bind_data->Cast<SystemCatalogPruningData>();
	if (data.predicates.empty()) {
		return Catalog::GetAllSchemas(context);
	}

	vector<reference<SchemaCatalogEntry>> result;
	string schema_lookup;
	bool have_lookup = TryGetSchemaNameEquality(context, data, schema_lookup);

	auto &db_manager = DatabaseManager::Get(context);
	for (auto &database : db_manager.GetDatabases(context)) {
		if (database->GetVisibility() == AttachVisibility::HIDDEN) {
			continue; // same skip as Catalog::GetAllSchemas
		}
		auto &catalog = database->GetCatalog();
		stats.catalogs_total++;

		unordered_map<idx_t, Value> known;
		if (data.database_name_idx.IsValid()) {
			known[data.database_name_idx.GetIndex()] = Value(catalog.GetName());
		}
		if (data.database_oid_idx.IsValid()) {
			known[data.database_oid_idx.GetIndex()] = Value::BIGINT(NumericCast<int64_t>(catalog.GetOid()));
		}
		if (Rejects(context, data.predicates, known)) {
			continue; // catalog-level pruning
		}
		stats.catalogs_scanned++;

		vector<reference<SchemaCatalogEntry>> candidates;
		bool listed = true;
		if (have_lookup) {
			// Point lookup instead of listing (for Glue, GetDatabase vs GetDatabases). It can fail where a listing
			// would not, so any non-internal error falls back to the full listing below.
			optional_ptr<SchemaCatalogEntry> entry;
			bool lookup_ok = true;
			try {
				entry = catalog.GetSchema(context, Identifier(schema_lookup), OnEntryNotFound::RETURN_NULL);
			} catch (InternalException &) {
				throw;
			} catch (std::exception &) {
				lookup_ok = false;
			}
			if (lookup_ok && !entry) {
				listed = false; // nothing named exactly that: this catalog contributes no candidates
			} else if (lookup_ok && entry->name.GetIdentifierName() == schema_lookup) {
				// Raw-string compare: Identifier::operator== is case-insensitive and would accept a
				// differently-cased sibling, breaking completeness.
				candidates.push_back(*entry);
				listed = false;
				stats.used_schema_lookup = true;
			}
			// else: lookup failed or returned a differently-cased entry; fall through to a full listing
		}
		if (listed) {
			candidates = catalog.GetSchemas(context);
		}
		for (auto &schema_ref : candidates) {
			auto &schema = schema_ref.get();
			stats.schemas_total++;
			auto schema_known = known;
			if (data.schema_name_idx.IsValid()) {
				schema_known[data.schema_name_idx.GetIndex()] = Value(schema.name);
			}
			if (data.schema_oid_idx.IsValid()) {
				schema_known[data.schema_oid_idx.GetIndex()] = Value::BIGINT(NumericCast<int64_t>(schema.oid));
			}
			if (Rejects(context, data.predicates, schema_known)) {
				continue; // schema-level pruning
			}
			stats.schemas_scanned++;
			result.push_back(schema);
		}
	}
	// same comparator as Catalog::GetAllSchemas: both operands are Identifier, so `<` is case-insensitive
	std::sort(result.begin(), result.end(), [](reference<SchemaCatalogEntry> l, reference<SchemaCatalogEntry> r) {
		auto &left = l.get();
		auto &right = r.get();
		if (left.catalog.GetName() < right.catalog.GetName()) {
			return true;
		}
		if (left.catalog.GetName() == right.catalog.GetName()) {
			return left.name < right.name;
		}
		return false;
	});
	return result;
}

InsertionOrderPreservingMap<string> SystemCatalogPruning::ToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	if (!input.bind_data) {
		return result;
	}
	auto &data = input.bind_data->Cast<SystemCatalogPruningData>();
	if (data.predicates.empty()) {
		return result;
	}
	string filters;
	for (idx_t i = 0; i < data.predicates.size(); i++) {
		if (i > 0) {
			filters += ", ";
		}
		filters += data.predicates[i]->ToString();
	}
	result["Catalog Filters"] = filters;
	return result;
}

void SystemCatalogPruning::ReportMetrics(TableFunctionGetMetricsInput &input, const SystemCatalogPruningStats &stats) {
	input.operator_metrics.AddExtraInfo("Catalogs Scanned",
	                                    to_string(stats.catalogs_scanned) + "/" + to_string(stats.catalogs_total));
	input.operator_metrics.AddExtraInfo("Schemas Scanned",
	                                    to_string(stats.schemas_scanned) + "/" + to_string(stats.schemas_total));
	if (stats.used_schema_lookup) {
		input.operator_metrics.AddExtraInfo("Schema Lookup", "true");
	}
}

void SystemCatalogPruning::RegisterPushdown(TableFunction &function) {
	function.pushdown_complex_filter = PushdownComplexFilter;
	function.to_string = ToString;
}

} // namespace duckdb
