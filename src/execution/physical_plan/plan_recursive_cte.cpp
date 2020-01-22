#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/execution/operator/scan/physical_cte_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_recursive_cte.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalRecursiveCTE &op) {
    assert(op.children.size() == 2);

    auto working_table = std::make_shared<ChunkCollection>();
    auto index = std::make_shared<index_t>();
    (*index) = 0;

    rec_ctes[op.table_index] = std::make_pair(working_table, index);

    auto left = CreatePlan(*op.children[0]);
    auto right = CreatePlan(*op.children[1]);

    auto cte = make_unique<PhysicalRecursiveCTE>(op, op.union_all, move(left), move(right));
    cte->working_table = working_table;
    cte->iteration = index;

    return move(cte);
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCTERef &op) {
    assert(op.children.size() == 0);

    auto chunk_scan = make_unique<PhysicalCTEScan>(op.types, PhysicalOperatorType::CHUNK_SCAN);

    auto cte = rec_ctes.find(op.table_index);
    chunk_scan->collection = cte->second.first.get();
    chunk_scan->remote_iteration = cte->second.second.get();
    return move(chunk_scan);
}
