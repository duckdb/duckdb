#include "duckdb/common/feature_serve.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/feature_query.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

#include <algorithm>

namespace duckdb {

//! Synthetic spine column injected in "latest" serving mode (no explicit ASOF clause): a constant +infinity
//! timestamp used as the ASOF probe so the join resolves each entity to its most recent snapshot.
static constexpr const char *SERVE_ASOF_PROBE_COLUMN = "__serve_asof_probe";

static optional_ptr<FeatureCatalogEntry> LookupFeature(ClientContext &context, const string &feature_name) {
	auto schemas = Catalog::GetAllSchemas(context);
	for (auto &schema : schemas) {
		auto entry = schema.get().GetEntry(schema.get().GetCatalogTransaction(context), CatalogType::FEATURE_ENTRY,
		                                   feature_name);
		if (entry) {
			return &entry->Cast<FeatureCatalogEntry>();
		}
	}
	return nullptr;
}

//! One retained snapshot: its version and the single timestamp every row of that version carries.
struct FeatureVersionStamp {
	int64_t version;
	timestamp_t timestamp;
};

//! The feature's retained snapshots, newest first: timestamp descending, ties broken by higher version.
//! REFRESH AT accepts arbitrary timestamps, so version order need not match timestamp order -- a backfill can
//! carry a higher version number with an older timestamp. Ordering by timestamp is what makes resolution
//! correct in that case; the version tiebreak is what makes it deterministic.
static vector<FeatureVersionStamp> RetainedVersionStamps(const FeatureCatalogEntry &feat) {
	vector<FeatureVersionStamp> stamps;
	for (idx_t i = 0; i < feat.retained_version_numbers.size(); i++) {
		FeatureVersionStamp stamp;
		stamp.version = feat.retained_version_numbers[i];
		stamp.timestamp = timestamp_t(feat.retained_version_timestamps_micros[i]);
		stamps.push_back(stamp);
	}
	std::sort(stamps.begin(), stamps.end(), [](const FeatureVersionStamp &a, const FeatureVersionStamp &b) {
		if (a.timestamp.value != b.timestamp.value) {
			return a.timestamp.value > b.timestamp.value;
		}
		return a.version > b.version;
	});
	return stamps;
}

//! Collapse stamps sharing a timestamp down to the highest version. Input must already be newest-first, so
//! duplicates are adjacent and the first of each run is the highest version.
//! This is required for correctness, not tidiness: DuckDB's ASOF join has no value-based tie-break among rows
//! sharing an identical order-by value -- which row wins depends on insertion order. Without this, two
//! snapshots refreshed AT the same timestamp would resolve unpredictably instead of to the newer version.
static vector<FeatureVersionStamp> DedupVersionStampsByTimestamp(const vector<FeatureVersionStamp> &stamps) {
	vector<FeatureVersionStamp> result;
	for (auto &stamp : stamps) {
		if (result.empty() || result.back().timestamp.value != stamp.timestamp.value) {
			result.push_back(stamp);
		}
	}
	return result;
}

//! Whether this statement can use the version-table path. Requires the setting to allow it and every served
//! feature to carry a retained version map -- a feature last refreshed before that map existed has an empty
//! one and must fall back to the legacy ASOF path. The decision is per-statement, not per-feature, so one
//! SERVE call never mixes the two plan shapes.
//! This is also what guarantees the version list is non-empty everywhere downstream.
static bool CanUseEquiJoinPath(ClientContext &context, const vector<ServeFeatureRequest> &features) {
	if (Settings::Get<FeatureServeLegacyAsofSetting>(context)) {
		return false;
	}
	for (auto &request : features) {
		auto feature_entry = LookupFeature(context, request.feature_name);
		if (!feature_entry || feature_entry->retained_version_numbers.empty()) {
			return false;
		}
	}
	return true;
}

//! Resolve a feature for serving, raising a clear error if it exists but has never been refreshed
//! (current_version == 0, so no version table has been materialized yet).
static FeatureCatalogEntry &ResolveServableFeature(ClientContext &context, const string &feature_name) {
	auto feature_entry = LookupFeature(context, feature_name);
	if (!feature_entry) {
		throw CatalogException("Feature \"%s\" does not exist", feature_name);
	}
	if (feature_entry->current_version < 1) {
		throw CatalogException("Feature \"%s\" has not been refreshed yet — run REFRESH FEATURE %s first", feature_name,
		                       feature_name);
	}
	return *feature_entry;
}

static unique_ptr<BaseTableRef> BaseTable(const string &table_name, const string &alias) {
	auto result = make_uniq<BaseTableRef>();
	result->table_name = table_name;
	result->alias = alias;
	return result;
}

static unique_ptr<ColumnRefExpression> ColumnRef(const string &alias, const string &column_name) {
	return make_uniq<ColumnRefExpression>(column_name, alias);
}

static unique_ptr<ParsedExpression> Conjoin(unique_ptr<ParsedExpression> left, unique_ptr<ParsedExpression> right) {
	return make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(left), std::move(right));
}

static bool ContainsColumn(const vector<string> &columns, const string &column) {
	for (auto &entry : columns) {
		if (entry == column) {
			return true;
		}
	}
	return false;
}

static vector<FeatureServeEntityMapping>
ResolveEntityMappings(const FeatureCatalogEntry &feat, const vector<FeatureServeEntityMapping> &feature_mappings,
                      const string &spine_entity_override) {
	if (feature_mappings.empty() && spine_entity_override.empty()) {
		vector<FeatureServeEntityMapping> result;
		result.reserve(feat.entity_columns.size());
		for (auto &feature_entity : feat.entity_columns) {
			result.push_back(FeatureServeEntityMapping {feature_entity, feature_entity});
		}
		return result;
	}

	if (feat.entity_columns.empty()) {
		throw BinderException("SERVE FEATURE entity mapping was provided for global feature \"%s\"", feat.name);
	}

	if (!spine_entity_override.empty()) {
		if (!feature_mappings.empty()) {
			throw BinderException("SERVE FEATURE cannot combine feature-specific ENTITY mappings with a global ENTITY "
			                      "override");
		}
		if (feat.entity_columns.size() > 1) {
			throw BinderException("SERVE FEATURE with global ENTITY override does not support feature \"%s\" with "
			                      "multiple entity columns",
			                      feat.name);
		}
		return vector<FeatureServeEntityMapping> {
		    FeatureServeEntityMapping {feat.entity_columns[0], spine_entity_override}};
	}

	if (feature_mappings.size() == 1 && feature_mappings[0].feature_column.empty()) {
		if (feat.entity_columns.size() > 1) {
			throw BinderException(
			    "SERVE FEATURE shorthand ENTITY mapping does not support feature \"%s\" with multiple "
			    "entity columns",
			    feat.name);
		}
		return vector<FeatureServeEntityMapping> {
		    FeatureServeEntityMapping {feat.entity_columns[0], feature_mappings[0].spine_column}};
	}

	vector<FeatureServeEntityMapping> result;
	result.reserve(feat.entity_columns.size());
	for (auto &feature_entity : feat.entity_columns) {
		result.push_back(FeatureServeEntityMapping {feature_entity, feature_entity});
	}
	for (auto &mapping : feature_mappings) {
		if (!ContainsColumn(feat.entity_columns, mapping.feature_column)) {
			throw BinderException("Feature \"%s\" has no entity column \"%s\"", feat.name, mapping.feature_column);
		}
		for (auto &resolved : result) {
			if (resolved.feature_column == mapping.feature_column) {
				resolved.spine_column = mapping.spine_column;
				break;
			}
		}
	}
	return result;
}

static unique_ptr<ParsedExpression> ServeJoinCondition(const string &feature_alias,
                                                       const vector<FeatureServeEntityMapping> &entity_mappings,
                                                       const string &spine_ts) {
	unique_ptr<ParsedExpression> condition;
	for (auto &mapping : entity_mappings) {
		auto entity_condition =
		    make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, ColumnRef("spine", mapping.spine_column),
		                                    ColumnRef(feature_alias, mapping.feature_column));
		condition =
		    condition ? Conjoin(std::move(condition), std::move(entity_condition)) : std::move(entity_condition);
	}
	// ASOF inequality: for each spine row pick the entity's most recent snapshot at or before the spine
	// timestamp, i.e. the greatest __feature_timestamp that does not exceed the spine's as-of time.
	auto timestamp_condition =
	    make_uniq<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, ColumnRef("spine", spine_ts),
	                                    ColumnRef(feature_alias, FEATURE_TIMESTAMP_COLUMN));
	return condition ? Conjoin(std::move(condition), std::move(timestamp_condition)) : std::move(timestamp_condition);
}

//! Alias for one feature's version-boundary table. Distinct per feature so a multi-feature SERVE can attach
//! several without collision.
static string ServeVersionTableAlias(const string &feature_alias) {
	return "vt_" + feature_alias;
}

//! An inline VALUES table of the feature's deduped retained (version, timestamp) pairs, with its columns named
//! __feature_version / __feature_timestamp. Bounded by retain_versions, so it is a handful of rows however
//! large the store is -- which is the whole point: the ASOF join sorts this instead of the store.
static unique_ptr<TableRef> VersionBoundaryTableRef(const vector<FeatureVersionStamp> &stamps, const string &alias) {
	auto table_ref = make_uniq<ExpressionListRef>();
	table_ref->alias = alias;
	table_ref->expected_names = {FEATURE_VERSION_COLUMN, FEATURE_TIMESTAMP_COLUMN};
	table_ref->expected_types = {LogicalType::BIGINT, LogicalType::TIMESTAMP};
	for (auto &stamp : stamps) {
		vector<unique_ptr<ParsedExpression>> row;
		row.push_back(make_uniq<ConstantExpression>(Value::BIGINT(stamp.version)));
		row.push_back(make_uniq<ConstantExpression>(Value::TIMESTAMP(stamp.timestamp)));
		table_ref->values.push_back(std::move(row));
	}
	return std::move(table_ref);
}

//! Attach an ASOF LEFT JOIN resolving which version applies to each spine row, so the joined table's own
//! __feature_version column can then be referenced directly by the store join.
//! No partition/equality condition: which version applies is a function of time alone, identical for every
//! entity, so this matches against a handful of rows rather than the store. spine_ts is the +infinity probe
//! column in latest mode and the user's as-of column in time-travel mode -- the construction is the same
//! either way, and the probe being >= every retained timestamp is exactly what makes latest mode resolve to
//! the newest snapshot. A spine row older than every snapshot resolves to NULL, matching ASOF LEFT semantics.
static void AttachVersionBoundaryJoin(unique_ptr<TableRef> &from_table, const string &version_alias,
                                      const vector<FeatureVersionStamp> &stamps, const string &spine_ts) {
	auto join = make_uniq<JoinRef>(JoinRefType::ASOF);
	join->type = JoinType::LEFT;
	join->left = std::move(from_table);
	join->right = VersionBoundaryTableRef(stamps, version_alias);
	join->condition =
	    make_uniq<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, ColumnRef("spine", spine_ts),
	                                    ColumnRef(version_alias, FEATURE_TIMESTAMP_COLUMN));
	from_table = std::move(join);
}

//! Store-side join condition: the entity keys plus an equality against the version the boundary join resolved.
//! A version holds exactly one row per entity, so this matches at most one store row per spine row -- the same
//! guarantee the legacy ASOF join gives. Global features have no entity columns, leaving the version equality
//! as the whole condition, which is correct: such a feature has one row per version.
static unique_ptr<ParsedExpression> ServeEquiJoinCondition(const string &feature_alias, const string &version_alias,
                                                           const vector<FeatureServeEntityMapping> &entity_mappings) {
	unique_ptr<ParsedExpression> condition;
	for (auto &mapping : entity_mappings) {
		auto entity_condition =
		    make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, ColumnRef("spine", mapping.spine_column),
		                                    ColumnRef(feature_alias, mapping.feature_column));
		condition =
		    condition ? Conjoin(std::move(condition), std::move(entity_condition)) : std::move(entity_condition);
	}
	auto version_condition =
	    make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, ColumnRef(version_alias, FEATURE_VERSION_COLUMN),
	                                    ColumnRef(feature_alias, FEATURE_VERSION_COLUMN));
	return condition ? Conjoin(std::move(condition), std::move(version_condition)) : std::move(version_condition);
}

//! MIN or MAX of the version the spine can reach, as a scalar subquery that independently re-derives the
//! version-boundary join over the raw spine table:
//!   (SELECT <aggregate>(vt.__feature_version)
//!      FROM (SELECT *, <probe> AS __bound_probe FROM <spine>) AS s
//!      ASOF LEFT JOIN <version values> vt ON s.__bound_probe >= vt.__feature_timestamp)
//! Self-contained: it re-wraps the spine itself rather than referencing the outer query's "spine" alias, so it
//! can be evaluated independently of the surrounding join tree.
//! DuckDB's ASOF inequality requires a real column on each side -- verified: a bare constant on the left
//! throws "Binder Error: Missing ASOF JOIN inequality". In latest mode there is no real spine column to use
//! (the spine has no as-of column of its own), so the probe is projected as one first, exactly the shape
//! SpineTableRef already uses for the same reason. In time-travel mode the user's as-of column already exists,
//! but it still goes through the same wrap so both modes share one construction.
static unique_ptr<ParsedExpression> ReachableVersionBound(const string &aggregate, const string &spine_table,
                                                          const string &spine_asof_column,
                                                          const vector<FeatureVersionStamp> &stamps,
                                                          const string &version_alias, bool latest_mode) {
	constexpr const char *BOUND_PROBE_COLUMN = "__bound_probe";

	auto inner = make_uniq<SelectNode>();
	inner->select_list.push_back(make_uniq<StarExpression>());
	unique_ptr<ParsedExpression> probe;
	if (latest_mode) {
		probe = make_uniq<ConstantExpression>(Value::TIMESTAMP(timestamp_t::infinity()));
	} else {
		probe = make_uniq<ColumnRefExpression>(spine_asof_column);
	}
	probe->SetAlias(BOUND_PROBE_COLUMN);
	inner->select_list.push_back(std::move(probe));
	inner->from_table = BaseTable(spine_table, string());
	auto spine_stmt = make_uniq<SelectStatement>();
	spine_stmt->node = std::move(inner);
	auto spine_ref = make_uniq<SubqueryRef>(std::move(spine_stmt), "s");

	auto join = make_uniq<JoinRef>(JoinRefType::ASOF);
	join->type = JoinType::LEFT;
	join->left = std::move(spine_ref);
	join->right = VersionBoundaryTableRef(stamps, version_alias);
	join->condition = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO,
	                                                  ColumnRef("s", BOUND_PROBE_COLUMN),
	                                                  ColumnRef(version_alias, FEATURE_TIMESTAMP_COLUMN));

	auto node = make_uniq<SelectNode>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(ColumnRef(version_alias, FEATURE_VERSION_COLUMN));
	node->select_list.push_back(make_uniq<FunctionExpression>(aggregate, std::move(children)));
	node->from_table = std::move(join);

	auto stmt = make_uniq<SelectStatement>();
	stmt->node = std::move(node);
	auto subquery = make_uniq<SubqueryExpression>();
	subquery->subquery_type = SubqueryType::SCALAR;
	subquery->subquery = std::move(stmt);
	return std::move(subquery);
}

//! The store side of the equi-join, bounded to the versions the spine can reach:
//!   (SELECT * FROM <store>
//!     WHERE __feature_version >= COALESCE(<min reachable>, <oldest retained>)
//!       AND __feature_version <= <max reachable>) AS <alias>
//! The lower bound is coalesced because a spine reaching back before every snapshot resolves to NULL there,
//! and a NULL bound would filter out every row. The upper bound is deliberately not coalesced: a NULL there
//! means no spine row resolves to any version at all, and an empty scan is then the correct result.
static unique_ptr<TableRef> ServeStoreRef(const string &store_table, const string &feature_alias,
                                          const vector<FeatureVersionStamp> &stamps, const string &spine_table,
                                          const string &spine_asof_column, bool latest_mode) {
	auto version_alias = ServeVersionTableAlias(feature_alias) + "_bound";

	int64_t oldest_version = stamps.front().version;
	for (auto &stamp : stamps) {
		oldest_version = MinValue<int64_t>(oldest_version, stamp.version);
	}

	auto lower = make_uniq<OperatorExpression>(
	    ExpressionType::OPERATOR_COALESCE,
	    ReachableVersionBound("min", spine_table, spine_asof_column, stamps, version_alias, latest_mode),
	    make_uniq<ConstantExpression>(Value::BIGINT(oldest_version)));
	auto lower_bound = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO,
	                                                   make_uniq<ColumnRefExpression>(string(FEATURE_VERSION_COLUMN)),
	                                                   std::move(lower));
	auto upper_bound = make_uniq<ComparisonExpression>(
	    ExpressionType::COMPARE_LESSTHANOREQUALTO, make_uniq<ColumnRefExpression>(string(FEATURE_VERSION_COLUMN)),
	    ReachableVersionBound("max", spine_table, spine_asof_column, stamps, version_alias, latest_mode));

	auto inner = make_uniq<SelectNode>();
	inner->select_list.push_back(make_uniq<StarExpression>());
	inner->from_table = BaseTable(store_table, string());
	inner->where_clause = Conjoin(std::move(lower_bound), std::move(upper_bound));

	auto stmt = make_uniq<SelectStatement>();
	stmt->node = std::move(inner);
	return make_uniq<SubqueryRef>(std::move(stmt), feature_alias);
}

static unique_ptr<StarExpression> FeatureStar(const string &feature_alias, const vector<string> &feature_entities) {
	auto result = make_uniq<StarExpression>(feature_alias);
	for (auto &feature_entity : feature_entities) {
		result->exclude_list.insert(QualifiedColumnName(feature_entity));
	}
	result->exclude_list.insert(QualifiedColumnName(FEATURE_VERSION_COLUMN));
	result->exclude_list.insert(QualifiedColumnName(FEATURE_TIMESTAMP_COLUMN));
	return result;
}

static bool IsPositiveInterval(const interval_t &interval) {
	return interval.months > 0 || interval.days > 0 || interval.micros > 0;
}

//! The feature's value columns: every column of its denormalized store table except the entity keys and the
//! two internal bookkeeping columns. Read from the store schema (the feature is refreshed, so it exists).
static vector<string> FeatureValueColumns(ClientContext &context, const FeatureCatalogEntry &feat) {
	auto store_name = FeatureStoreTableName(feat.name);
	optional_ptr<CatalogEntry> entry;
	for (auto &schema : Catalog::GetAllSchemas(context)) {
		entry =
		    schema.get().GetEntry(schema.get().GetCatalogTransaction(context), CatalogType::TABLE_ENTRY, store_name);
		if (entry) {
			break;
		}
	}
	if (!entry) {
		throw CatalogException("Feature store table \"%s\" does not exist", store_name);
	}
	auto &store = entry->Cast<TableCatalogEntry>();
	vector<string> result;
	for (auto &col : store.GetColumns().Logical()) {
		auto &name = col.Name();
		if (ContainsColumn(feat.entity_columns, name) || name == FEATURE_VERSION_COLUMN ||
		    name == FEATURE_TIMESTAMP_COLUMN) {
			continue;
		}
		result.push_back(name);
	}
	return result;
}

//! The staleness reference the TTL is measured against. In time-travel mode (explicit ASOF) this is the
//! spine request timestamp; in latest mode (no ASOF, the probe is +infinity) it is wall-clock now().
static unique_ptr<ParsedExpression> TTLReferenceTime(const string &spine_ts, bool latest_mode) {
	if (!latest_mode) {
		return ColumnRef("spine", spine_ts);
	}
	// CAST(now() AS TIMESTAMP) — now() is TIMESTAMPTZ; the store timestamp is TIMESTAMP, so cast to match.
	auto now_call = make_uniq<FunctionExpression>("now", vector<unique_ptr<ParsedExpression>>());
	return make_uniq<CastExpression>(LogicalType::TIMESTAMP, std::move(now_call));
}

//! Append the projected feature columns for one served feature. With no TTL configured this is a single
//! star (excluding the entity keys and internal columns). With a TTL, each value column is wrapped so that a
//! snapshot older than the TTL relative to the reference time resolves to NULL:
//!   CASE WHEN f.__feature_timestamp >= <ttl_reference> - INTERVAL <ttl> THEN f.<col> END AS <col>
//! The ASOF join already picks the freshest matched snapshot, so testing that single timestamp is sufficient.
static void AddFeatureProjections(vector<unique_ptr<ParsedExpression>> &select_list, ClientContext &context,
                                  const FeatureCatalogEntry &feat, const string &feature_alias, const string &spine_ts,
                                  bool latest_mode) {
	if (!IsPositiveInterval(feat.ttl_interval)) {
		select_list.push_back(FeatureStar(feature_alias, feat.entity_columns));
		return;
	}
	for (auto &value_column : FeatureValueColumns(context, feat)) {
		// <ttl_reference> - INTERVAL <ttl>
		vector<unique_ptr<ParsedExpression>> minus_children;
		minus_children.push_back(TTLReferenceTime(spine_ts, latest_mode));
		minus_children.push_back(make_uniq<ConstantExpression>(Value::INTERVAL(feat.ttl_interval)));
		auto stale_threshold =
		    make_uniq<FunctionExpression>("-", std::move(minus_children), nullptr, nullptr, false, true);

		auto fresh = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO,
		                                             ColumnRef(feature_alias, FEATURE_TIMESTAMP_COLUMN),
		                                             std::move(stale_threshold));

		auto case_expr = make_uniq<CaseExpression>();
		CaseCheck check;
		check.when_expr = std::move(fresh);
		check.then_expr = ColumnRef(feature_alias, value_column);
		case_expr->case_checks.push_back(std::move(check));
		case_expr->else_expr = make_uniq<ConstantExpression>(Value());
		case_expr->SetAlias(value_column);
		select_list.push_back(std::move(case_expr));
	}
}

//! The store side of the ASOF join. In time-travel mode we wrap the store table so the scan only reads
//! snapshots that could satisfy the ASOF inequality: any row whose __feature_timestamp exceeds the greatest
//! spine as-of time can never be a match (the join condition is spine.ts >= store.__feature_timestamp), so
//! dropping it is result-preserving. Because the store is physically clustered by __feature_timestamp — each
//! REFRESH appends one snapshot at a strictly later timestamp — this upper bound lets the store scan skip whole
//! row groups via zonemaps. The bound is a scalar subquery over the spine, so it costs one extra spine
//! aggregation and prunes to exactly the versions at or before the latest request:
//!   (SELECT * FROM <store> WHERE __feature_timestamp <= (SELECT max(<spine_asof>) FROM <spine>)) AS <alias>
//! Latest mode probes with +infinity (no upper bound prunes anything), so it reads the store table directly.
static unique_ptr<TableRef> ServeStoreTableRef(const string &store_table, const string &feature_alias,
                                               const string &spine_table, const string &spine_asof_column,
                                               bool latest_mode) {
	if (latest_mode) {
		return BaseTable(store_table, feature_alias);
	}

	// (SELECT max(<spine_asof_column>) FROM <spine_table>)
	auto max_node = make_uniq<SelectNode>();
	vector<unique_ptr<ParsedExpression>> max_children;
	max_children.push_back(make_uniq<ColumnRefExpression>(spine_asof_column));
	max_node->select_list.push_back(make_uniq<FunctionExpression>("max", std::move(max_children)));
	max_node->from_table = BaseTable(spine_table, string());
	auto max_stmt = make_uniq<SelectStatement>();
	max_stmt->node = std::move(max_node);
	auto max_subquery = make_uniq<SubqueryExpression>();
	max_subquery->subquery_type = SubqueryType::SCALAR;
	max_subquery->subquery = std::move(max_stmt);

	auto inner = make_uniq<SelectNode>();
	inner->select_list.push_back(make_uniq<StarExpression>());
	inner->from_table = BaseTable(store_table, string());
	inner->where_clause = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_LESSTHANOREQUALTO,
	                                                      make_uniq<ColumnRefExpression>(FEATURE_TIMESTAMP_COLUMN),
	                                                      std::move(max_subquery));

	auto stmt = make_uniq<SelectStatement>();
	stmt->node = std::move(inner);
	return make_uniq<SubqueryRef>(std::move(stmt), feature_alias);
}

static void AttachServeJoin(unique_ptr<TableRef> &from_table, const FeatureCatalogEntry &feat,
                            const string &feature_alias, const vector<FeatureServeEntityMapping> &feature_mappings,
                            const string &spine_entity_override, const string &spine_ts, const string &spine_table,
                            const string &spine_asof_column, bool latest_mode) {
	// Serve from the denormalized store table via an ASOF join: every retained version is present, and the
	// join resolves each spine row to the entity's latest snapshot at or before the spine's as-of time (in
	// latest mode the as-of time is the +infinity probe, so this resolves to the entity's newest snapshot).
	auto store_table = FeatureStoreTableName(feat.name);
	auto entity_mappings = ResolveEntityMappings(feat, feature_mappings, spine_entity_override);

	auto join = make_uniq<JoinRef>(JoinRefType::ASOF);
	join->type = JoinType::LEFT;
	join->left = std::move(from_table);
	join->right = ServeStoreTableRef(store_table, feature_alias, spine_table, spine_asof_column, latest_mode);
	join->condition = ServeJoinCondition(feature_alias, entity_mappings, spine_ts);
	from_table = std::move(join);
}

//! The spine relation the features are served for. In time-travel mode this is just the spine table. In latest
//! mode we wrap it so it carries a synthetic +infinity ASOF probe column: SELECT *, TIMESTAMP 'infinity' AS
//! __serve_asof_probe FROM <spine>. A DuckDB ASOF inequality must reference a column from each side, so the
//! probe has to be a real column rather than an inline constant.
static unique_ptr<TableRef> SpineTableRef(const string &spine_table, bool latest_mode) {
	if (!latest_mode) {
		return BaseTable(spine_table, "spine");
	}
	auto inner = make_uniq<SelectNode>();
	inner->select_list.push_back(make_uniq<StarExpression>());
	auto probe = make_uniq<ConstantExpression>(Value::TIMESTAMP(timestamp_t::infinity()));
	probe->SetAlias(SERVE_ASOF_PROBE_COLUMN);
	inner->select_list.push_back(std::move(probe));
	inner->from_table = BaseTable(spine_table, string());

	auto stmt = make_uniq<SelectStatement>();
	stmt->node = std::move(inner);
	return make_uniq<SubqueryRef>(std::move(stmt), "spine");
}

//! The spine passthrough projection (spine.*), excluding the synthetic probe column in latest mode.
static unique_ptr<StarExpression> SpineStar(bool latest_mode) {
	auto star = make_uniq<StarExpression>("spine");
	if (latest_mode) {
		star->exclude_list.insert(QualifiedColumnName(SERVE_ASOF_PROBE_COLUMN));
	}
	return star;
}

unique_ptr<SelectStatement> BuildServeFeatureSelect(ClientContext &context, const vector<ServeFeatureRequest> &features,
                                                    const string &spine_table, const string &spine_entity_override,
                                                    const string &spine_asof_column) {
	auto schemas = Catalog::GetAllSchemas(context);
	bool spine_found = false;
	for (auto &schema : schemas) {
		auto entry =
		    schema.get().GetEntry(schema.get().GetCatalogTransaction(context), CatalogType::TABLE_ENTRY, spine_table);
		if (entry) {
			spine_found = true;
			break;
		}
	}
	if (!spine_found) {
		throw CatalogException("Spine table \"%s\" does not exist", spine_table);
	}

	// With an explicit ASOF clause we time-travel against that spine column; without one we serve the latest
	// version of each feature by probing the ASOF join with a synthetic +infinity timestamp column.
	bool latest_mode = spine_asof_column.empty();
	string spine_ts = latest_mode ? SERVE_ASOF_PROBE_COLUMN : spine_asof_column;
	// One path serves both modes: spine_ts is the +infinity probe in latest mode and the user's as-of column
	// otherwise, and the version-boundary join treats them identically.
	bool use_equijoin = CanUseEquiJoinPath(context, features);

	auto select = make_uniq<SelectNode>();
	select->select_list.push_back(SpineStar(latest_mode));
	select->from_table = SpineTableRef(spine_table, latest_mode);

	for (idx_t i = 0; i < features.size(); i++) {
		auto &request = features[i];
		auto &feat = ResolveServableFeature(context, request.feature_name);
		auto alias = features.size() == 1 ? string("f") : "f" + duckdb::to_string(i);

		AddFeatureProjections(select->select_list, context, feat, alias, spine_ts, latest_mode);
		if (use_equijoin) {
			auto entity_mappings = ResolveEntityMappings(feat, request.entity_mappings, spine_entity_override);
			auto stamps = DedupVersionStampsByTimestamp(RetainedVersionStamps(feat));
			auto version_alias = ServeVersionTableAlias(alias);
			// Resolve the version first, then reach the store with it. Each feature appends its own pair of
			// joins onto the running chain, so a multi-feature SERVE just repeats this.
			AttachVersionBoundaryJoin(select->from_table, version_alias, stamps, spine_ts);
			auto join = make_uniq<JoinRef>(JoinRefType::REGULAR);
			join->type = JoinType::LEFT;
			join->left = std::move(select->from_table);
			join->right = ServeStoreRef(FeatureStoreTableName(feat.name), alias, stamps, spine_table, spine_asof_column,
			                            latest_mode);
			join->condition = ServeEquiJoinCondition(alias, version_alias, entity_mappings);
			select->from_table = std::move(join);
		} else {
			AttachServeJoin(select->from_table, feat, alias, request.entity_mappings, spine_entity_override, spine_ts,
			                spine_table, spine_asof_column, latest_mode);
		}
	}

	auto result = make_uniq<SelectStatement>();
	result->node = std::move(select);
	return result;
}

} // namespace duckdb
