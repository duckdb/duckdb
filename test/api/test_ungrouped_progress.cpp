#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

using namespace duckdb;

namespace {
class AggregateProgressEvent : public Event {
public:
	explicit AggregateProgressEvent(Executor &executor) : Event(executor) {
	}
	void Schedule() override {
	}
};

struct AggregateProgressProbe {
	optional_ptr<ClientContext> context;
	optional_ptr<PhysicalUngroupedAggregate> op;
	optional_ptr<GlobalSourceState> source;
	bool fail = false;
	idx_t calls = 0;
};

struct AggregateProgressBindData : FunctionData {
	explicit AggregateProgressBindData(shared_ptr<AggregateProgressProbe> probe_p) : probe(std::move(probe_p)) {
	}
	shared_ptr<AggregateProgressProbe> probe;
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<AggregateProgressBindData>(probe);
	}
	bool Equals(const FunctionData &other) const override {
		return probe == other.Cast<AggregateProgressBindData>().probe;
	}
};

idx_t StateSize(AggregateStateInput &) {
	return sizeof(int64_t);
}
void Initialize(AggregateStateInput &, data_ptr_t *, idx_t) {
}
void Update(Vector[], AggregateInputData &, idx_t, Vector &, idx_t) {
}
void Combine(Vector &, Vector &, AggregateInputData &, idx_t) {
}
void Finalize(Vector &, AggregateFinalizeInputData &input, Vector &result, idx_t count, idx_t offset) {
	auto &probe = *input.bind_data->Cast<AggregateProgressBindData>().probe;
	auto progress = probe.op->GetProgress(*probe.context, *probe.source);
	REQUIRE(progress.IsValid());
	REQUIRE(progress.done == 0);
	REQUIRE(progress.total == 1);
	probe.calls++;
	if (probe.fail) {
		throw InvalidInputException("aggregate progress finalization failure");
	}
	auto writer = FlatVector::Writer<int64_t>(result, count, offset);
	for (idx_t i = 0; i < count; i++) {
		writer.WriteValue(42);
	}
}
} // namespace

TEST_CASE("Ungrouped aggregate source progress waits for successful value finalization", "[progress-bar]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;
	PhysicalPlan plan(Allocator::Get(context));
	auto probe = make_shared_ptr<AggregateProgressProbe>();
	AggregateFunction function("progress_probe", {}, LogicalType::BIGINT, StateSize, Initialize, Update, Combine,
	                           Finalize, FunctionNullHandling::SPECIAL_HANDLING);
	vector<unique_ptr<Expression>> aggregates;
	aggregates.push_back(make_uniq<BoundAggregateExpression>(
	    BoundAggregateFunction(function), vector<unique_ptr<Expression>> {}, nullptr,
	    make_uniq<AggregateProgressBindData>(probe), AggregateType::NON_DISTINCT));
	PhysicalUngroupedAggregate op(plan, {LogicalType::BIGINT}, std::move(aggregates), 1,
	                              TupleDataValidityType::CAN_HAVE_NULL_VALUES);
	probe->context = context;
	probe->op = op;
	op.sink_state = op.GetGlobalSinkState(context);
	ThreadContext thread(context);
	ExecutionContext execution(context, thread, nullptr);
	InterruptState interrupt;
	Executor executor(context);
	Pipeline pipeline(executor);
	AggregateProgressEvent event(executor);
	OperatorSinkFinalizeInput sink_input {*op.sink_state, interrupt};
	REQUIRE(op.Finalize(pipeline, event, context, sink_input) == SinkFinalizeType::READY);

	for (bool fail : {false, true, false}) {
		auto source = op.GetGlobalSourceState(context);
		probe->source = *source;
		probe->fail = fail;
		auto local = op.GetLocalSourceState(execution, *source);
		OperatorSourceInput source_input {*source, *local, interrupt};
		DataChunk result;
		result.Initialize(context, {LogicalType::BIGINT});
		auto before = op.GetProgress(context, *source);
		REQUIRE(before.IsValid());
		REQUIRE(before.done == 0);
		REQUIRE(before.total == 1);
		if (fail) {
			REQUIRE_THROWS_WITH(op.GetDataInternal(execution, result, source_input),
			                    Catch::Matchers::Contains("aggregate progress finalization failure"));
		} else {
			REQUIRE(op.GetDataInternal(execution, result, source_input) == SourceResultType::FINISHED);
			REQUIRE(result.size() == 1);
			REQUIRE(result.GetValue(0, 0) == Value::BIGINT(42));
		}
		auto after = op.GetProgress(context, *source);
		REQUIRE(after.IsValid());
		REQUIRE(after.done == (fail ? 0 : 1));
		REQUIRE(after.total == 1);
	}
	REQUIRE(probe->calls == 3);
}
