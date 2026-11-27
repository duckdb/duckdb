#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"
#include "duckdb/execution/operator/set/physical_cte.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parallel/pipeline_broadcast_exchange.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_recursive_cte.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

using namespace duckdb;

namespace {

enum class PlanningMutation { REPLACE_BODY, ADD_SIDE_EFFECTS, ADD_RECURSION };

unique_ptr<LogicalOperator> PlanningDummy(idx_t index) {
	return make_uniq<LogicalDummyScan>(TableIndex(index));
}

unique_ptr<LogicalOperator> MismatchedPlanningUnion() {
	auto left = PlanningDummy(20);
	auto right = PlanningDummy(21);
	left->types = {LogicalType::INTEGER};
	right->types = {LogicalType::VARCHAR};
	auto result = make_uniq<LogicalSetOperation>(TableIndex(22), 1, std::move(left), std::move(right),
	                                             LogicalOperatorType::LOGICAL_UNION, true);
	result->types = {LogicalType::INTEGER};
	return std::move(result);
}

class MutablePlanningCatalog : public DuckCatalog {
public:
	explicit MutablePlanningCatalog(AttachedDatabase &db) : DuckCatalog(db) {
	}

	using DuckCatalog::PlanDelete;

	PhysicalOperator &PlanDelete(ClientContext &, PhysicalPlanGenerator &planner, LogicalDelete &op) override {
		if (op.children[0]->type != LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
			return planner.Make<PhysicalDummyScan>(op.types, 1);
		}
		auto invalid = MismatchedPlanningUnion();
		REQUIRE_THROWS_WITH(planner.CreatePlan(*invalid), Catch::Contains("Type mismatch for SET OPERATION"));

		auto &cte = op.children[0]->Cast<LogicalMaterializedCTE>();
		switch (mutation) {
		case PlanningMutation::REPLACE_BODY:
			cte.children[0] = make_uniq<LogicalCrossProduct>(PlanningDummy(10), PlanningDummy(11));
			break;
		case PlanningMutation::ADD_SIDE_EFFECTS: {
			auto deletion = make_uniq<LogicalDelete>(op.table, TableIndex(10));
			deletion->children.push_back(PlanningDummy(11));
			cte.children[0]->children[0] = std::move(deletion);
			break;
		}
		case PlanningMutation::ADD_RECURSION: {
			auto recursive =
			    make_uniq<LogicalRecursiveCTE>("recursive", TableIndex(10), 1, true, vector<unique_ptr<Expression>>(),
			                                   PlanningDummy(11), PlanningDummy(12));
			recursive->ref_recurring = false;
			recursive->internal_types = {LogicalType::INTEGER};
			cte.children[0]->children[0] = std::move(recursive);
			break;
		}
		}
		cte.ResolveOperatorTypes();
		auto &physical_cte = planner.CreatePlan(*op.children[0]).Cast<PhysicalCTE>();
		replanned = true;
		has_side_effects = physical_cte.cte_body_has_side_effects;
		uses_exchange = bool(physical_cte.exchange);
		runs_to_completion = physical_cte.exchange && physical_cte.exchange->RunToCompletion();
		return planner.Make<PhysicalDummyScan>(op.types, 1);
	}

	PlanningMutation mutation = PlanningMutation::REPLACE_BODY;
	bool replanned = false;
	bool has_side_effects = false;
	bool uses_exchange = false;
	bool runs_to_completion = false;
};

struct MutablePlanningStorage : StorageExtension {
	MutablePlanningStorage() {
		attach = [](optional_ptr<StorageExtensionInfo>, ClientContext &, AttachedDatabase &db, const string &,
		            AttachInfo &info, AttachOptions &) -> unique_ptr<Catalog> {
			info.path = IN_MEMORY_PATH;
			return make_uniq_base<Catalog, MutablePlanningCatalog>(db);
		};
		create_transaction_manager = [](optional_ptr<StorageExtensionInfo>, AttachedDatabase &db,
		                                Catalog &) -> unique_ptr<TransactionManager> {
			return make_uniq<DuckTransactionManager>(db);
		};
	}
};

} // namespace

TEST_CASE("Catalog planning can replace logical descendants before re-entry", "[physical-plan][cte]") {
	DBConfig config;
	StorageExtension::Register(config, "mutable_planning", make_shared_ptr<MutablePlanningStorage>());
	DuckDB db(nullptr, &config);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("ATTACH 'mutable' AS mutable (TYPE mutable_planning)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE mutable.t(i INTEGER)"));
	con.BeginTransaction();
	auto &table = Catalog::GetEntry<TableCatalogEntry>(*con.context, QualifiedName::Parse("mutable.main.t"));
	auto &catalog = static_cast<MutablePlanningCatalog &>(table.catalog);

	SECTION("Replace the entire CTE body") {
		catalog.mutation = PlanningMutation::REPLACE_BODY;
	}
	SECTION("Add side effects below an existing body") {
		catalog.mutation = PlanningMutation::ADD_SIDE_EFFECTS;
	}
	SECTION("Add recursion below an existing body") {
		catalog.mutation = PlanningMutation::ADD_RECURSION;
	}

	auto body = make_uniq<LogicalCrossProduct>(PlanningDummy(1), PlanningDummy(2));
	auto cte = make_uniq<LogicalMaterializedCTE>("materialized", TableIndex(3), 2, std::move(body), PlanningDummy(4),
	                                             CTEMaterialize::CTE_MATERIALIZE_ALWAYS);
	auto deletion = make_uniq<LogicalDelete>(table, TableIndex(5));
	deletion->children.push_back(std::move(cte));
	unique_ptr<LogicalOperator> root = make_uniq<LogicalCrossProduct>(std::move(deletion), PlanningDummy(6));
	root->ResolveOperatorTypes();
	PhysicalPlanGenerator planner(*con.context);
	planner.ArenaRef();
	planner.CreatePlan(*root);
	REQUIRE(catalog.replanned);
	REQUIRE(catalog.has_side_effects == (catalog.mutation == PlanningMutation::ADD_SIDE_EFFECTS));
	REQUIRE(catalog.uses_exchange == (catalog.mutation != PlanningMutation::ADD_RECURSION));
	REQUIRE(catalog.runs_to_completion == (catalog.mutation == PlanningMutation::ADD_SIDE_EFFECTS));
	con.Rollback();
}
