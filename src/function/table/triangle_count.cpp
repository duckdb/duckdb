//===----------------------------------------------------------------------===//
// triangle_count('edges', 'src', 'dst') rewrites itself into
//   SELECT __triangle_count_impl(list({'src': ..., 'dst': ...})) FROM edges
// and counts triangles with degree-ordered forward-list intersection
// (Chiba & Nishizeki): O(m * sqrt(m)) time, O(n + m) memory.
// The whole edge list is materialized in memory; it does not spill.
//===----------------------------------------------------------------------===//

#include "duckdb/function/table/triangle_count.hpp"

#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

#include <algorithm>
#include <numeric>

namespace duckdb {

namespace {

struct Edge {
	int64_t src;
	int64_t dst;
};

void CheckInterrupt(optional_ptr<ClientContext> context) {
	if (context) {
		context->InterruptCheck();
	}
}

template <class T>
void ReleaseMemory(vector<T> &vec) {
	vector<T>().swap(vec);
}

//! Vertices relabelled by (degree, id) rank; fwd[offset[u]..offset[u+1]) are the
//! neighbours of u with a higher rank, sorted ascending.
template <class V>
struct ForwardGraph {
	vector<idx_t> offset;
	vector<V> fwd;

	idx_t VertexCount() const {
		return offset.size() - 1;
	}
};

//! Builds the forward graph; consumes `edges` and `ids` to keep peak memory down.
template <class V>
ForwardGraph<V> BuildForwardGraph(vector<Edge> &edges, vector<int64_t> &ids, optional_ptr<ClientContext> context) {
	const auto n = ids.size();
	ForwardGraph<V> graph;

	// Dense (low, high) pairs without self-loops.
	vector<std::pair<V, V>> pairs;
	pairs.reserve(edges.size());
	for (auto &edge : edges) {
		auto a = NumericCast<V>(std::lower_bound(ids.begin(), ids.end(), edge.src) - ids.begin());
		auto b = NumericCast<V>(std::lower_bound(ids.begin(), ids.end(), edge.dst) - ids.begin());
		if (a == b) {
			continue;
		}
		if (a > b) {
			std::swap(a, b);
		}
		pairs.emplace_back(a, b);
	}
	ReleaseMemory(edges);
	ReleaseMemory(ids);
	CheckInterrupt(context);

	std::sort(pairs.begin(), pairs.end());
	pairs.erase(std::unique(pairs.begin(), pairs.end()), pairs.end());
	CheckInterrupt(context);

	// Rank by ascending (degree, id); the degree order is what bounds the forward lists.
	vector<V> degree(n, 0);
	for (auto &pair : pairs) {
		degree[pair.first]++;
		degree[pair.second]++;
	}
	vector<V> order(n);
	std::iota(order.begin(), order.end(), V(0));
	std::sort(order.begin(), order.end(),
	          [&degree](V a, V b) { return degree[a] != degree[b] ? degree[a] < degree[b] : a < b; });
	ReleaseMemory(degree);
	vector<V> rank(n);
	for (idx_t i = 0; i < n; i++) {
		rank[order[i]] = NumericCast<V>(i);
	}
	ReleaseMemory(order);

	// Relabel by rank and orient low -> high, so one sort yields rank-sorted forward lists.
	for (auto &pair : pairs) {
		const auto a = rank[pair.first];
		const auto b = rank[pair.second];
		pair = a < b ? std::make_pair(a, b) : std::make_pair(b, a);
	}
	ReleaseMemory(rank);
	CheckInterrupt(context);
	std::sort(pairs.begin(), pairs.end());
	CheckInterrupt(context);

	graph.offset.assign(n + 1, 0);
	graph.fwd.reserve(pairs.size());
	for (auto &pair : pairs) {
		graph.offset[pair.first + 1]++;
		graph.fwd.push_back(pair.second);
	}
	for (idx_t i = 0; i < n; i++) {
		graph.offset[i + 1] += graph.offset[i];
	}
	return graph;
}

//! Counts the triangles whose lowest-ranked vertex lies in [begin, end).
template <class V>
int64_t CountRange(const ForwardGraph<V> &graph, idx_t begin, idx_t end, optional_ptr<ClientContext> context) {
	const auto &offset = graph.offset;
	const auto &fwd = graph.fwd;
	int64_t count = 0;
	for (idx_t u = begin; u < end; u++) {
		CheckInterrupt(context);
		const auto u_end = offset[u + 1];
		for (auto u_idx = offset[u]; u_idx < u_end; u_idx++) {
			const idx_t w = fwd[u_idx];
			// fwd(w) only holds vertices above w, so start the merge just past w
			auto i = u_idx + 1;
			auto j = offset[w];
			const auto j_end = offset[w + 1];
			while (i < u_end && j < j_end) {
				if (fwd[i] == fwd[j]) {
					count++;
					i++;
					j++;
				} else if (fwd[i] < fwd[j]) {
					i++;
				} else {
					j++;
				}
			}
		}
	}
	return count;
}

template <class V>
struct TriangleCountState {
	TriangleCountState(const ForwardGraph<V> &graph, optional_ptr<ClientContext> context, idx_t chunk)
	    : graph(graph), context(context), chunk(chunk) {
	}

	const ForwardGraph<V> &graph;
	optional_ptr<ClientContext> context;
	const idx_t chunk;
	atomic<idx_t> next_vertex {0};
	atomic<int64_t> count {0};
};

template <class V>
class TriangleCountTask : public BaseExecutorTask {
public:
	TriangleCountTask(TaskExecutor &executor, TriangleCountState<V> &state) : BaseExecutorTask(executor), state(state) {
	}

	void ExecuteTask() override {
		const auto n = state.graph.VertexCount();
		int64_t local = 0;
		// Work per vertex varies with degree, so claim small chunks on demand.
		while (!executor.HasError()) {
			const auto begin = state.next_vertex.fetch_add(state.chunk);
			if (begin >= n) {
				break;
			}
			local += CountRange(state.graph, begin, MinValue<idx_t>(begin + state.chunk, n), state.context);
		}
		state.count += local;
	}

	string TaskType() const override {
		return "TriangleCountTask";
	}

private:
	TriangleCountState<V> &state;
};

template <class V>
int64_t CountTrianglesDense(vector<Edge> &edges, vector<int64_t> &ids, optional_ptr<ClientContext> context,
                            idx_t num_threads) {
	const auto graph = BuildForwardGraph<V>(edges, ids, context);
	const auto n = graph.VertexCount();

	// Below this size scheduling costs more than the scan itself.
	static constexpr idx_t PARALLEL_THRESHOLD = 4096;
	if (!context || num_threads <= 1 || graph.fwd.size() < PARALLEL_THRESHOLD) {
		return CountRange(graph, 0, n, context);
	}

	TriangleCountState<V> state(graph, context, MaxValue<idx_t>(n / (num_threads * 16), 64));
	TaskExecutor executor(*context);
	vector<unique_ptr<BaseExecutorTask>> tasks;
	for (idx_t t = 0; t < num_threads; t++) {
		tasks.push_back(make_uniq<TriangleCountTask<V>>(executor, state));
	}
	executor.ScheduleTasks(std::move(tasks));
	executor.WorkOnTasks();
	return state.count;
}

//! Count the triangles of the undirected graph formed by `edges` (consumed). Vertex ids
//! are arbitrary int64 values. Self-loops and duplicate or reversed edges are ignored.
int64_t CountTriangles(vector<Edge> &edges, optional_ptr<ClientContext> context, idx_t num_threads) {
	vector<int64_t> ids;
	ids.reserve(edges.size() * 2);
	for (auto &edge : edges) {
		ids.push_back(edge.src);
		ids.push_back(edge.dst);
	}
	std::sort(ids.begin(), ids.end());
	ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
	if (ids.size() < 3) {
		return 0;
	}
	// 32-bit vertex labels halve the graph's footprint whenever they suffice.
	if (ids.size() <= NumericLimits<uint32_t>::Maximum()) {
		return CountTrianglesDense<uint32_t>(edges, ids, context, num_threads);
	}
	return CountTrianglesDense<idx_t>(edges, ids, context, num_threads);
}

//! __triangle_count_impl(LIST(STRUCT(src BIGINT, dst BIGINT))) -> BIGINT
//! A single STRUCT keeps src and dst paired, which two list() aggregates would not.
void TriangleCountImplFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto row_count = args.size();
	optional_ptr<ClientContext> context;
	idx_t num_threads = 1;
	if (state.HasClientContext()) {
		context = &state.GetClientContext();
		num_threads = NumericCast<idx_t>(TaskScheduler::GetScheduler(*context).NumberOfThreads());
	}
	auto &list_vec = args.data[0];
	// the list iterator does not accept a dictionary source
	list_vec.Flatten();

	auto edge_lists = list_vec.Values<VectorListType<VectorStructType<int64_t, int64_t>>>();
	auto writer = FlatVector::Writer<int64_t>(result, row_count);
	for (auto list_entry : edge_lists) {
		if (!list_entry.IsValid()) {
			writer.WriteNull();
			continue;
		}
		vector<Edge> edges;
		edges.reserve(list_entry.GetListLength());
		for (auto edge : list_entry.GetChildValues()) {
			auto src = edge.GetChildValue<0>();
			auto dst = edge.GetChildValue<1>();
			if (!edge.IsValid() || !src.IsValid() || !dst.IsValid()) {
				continue;
			}
			edges.push_back(Edge {src.GetValue(), dst.GetValue()});
		}
		writer.WriteValue(CountTriangles(edges, context, num_threads));
	}
}

string GetRequiredString(const Value &value, const string &argument) {
	if (value.IsNull()) {
		throw BinderException("triangle_count: %s cannot be NULL", argument);
	}
	auto result = value.ToString();
	if (result.empty()) {
		throw BinderException("triangle_count: %s cannot be empty", argument);
	}
	return result;
}

//! Rewrite triangle_count(tbl, src, dst) into a scan of tbl that folds every
//! edge into one list and hands it to __triangle_count_impl.
unique_ptr<TableRef> TriangleCountBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	auto table_name = GetRequiredString(input.inputs[0], "table name");
	auto src_column = GetRequiredString(input.inputs[1], "source column");
	auto dst_column = GetRequiredString(input.inputs[2], "destination column");

	auto qualified_name = QualifiedName::Parse(table_name);

	auto query =
	    // an empty edge table makes list() return NULL, which means zero triangles
	    StringUtil::Format("SELECT COALESCE(__triangle_count_impl(list({'src': CAST(%s AS BIGINT), 'dst': CAST(%s AS "
	                       "BIGINT)})), 0) AS triangle_count FROM %s",
	                       SQLIdentifier(src_column), SQLIdentifier(dst_column),
	                       qualified_name.ToString(QualifiedNameToStringMode::HIDE_DEFAULT_SCHEMA));

	Parser parser(context);
	parser.ParseQuery(query);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw BinderException("triangle_count: could not build the edge-scan query");
	}
	auto select = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
	auto result = make_uniq<SubqueryRef>(std::move(select));
	result->alias = "triangle_count";
	return std::move(result);
}

} // namespace

void TriangleCountTableFunction::RegisterFunction(BuiltinFunctions &set) {
	// Internal worker, not meant to be called directly.
	auto edge_list_type =
	    LogicalType::LIST(LogicalType::STRUCT({{"src", LogicalType::BIGINT}, {"dst", LogicalType::BIGINT}}));
	ScalarFunction impl("__triangle_count_impl", {}, LogicalType::BIGINT, TriangleCountImplFunction);
	impl.GetSignature().AddParameter("edges", edge_list_type);
	set.AddFunction(impl);

	TableFunction triangle_count("triangle_count", {}, nullptr, nullptr);
	triangle_count.GetSignature()
	    .AddParameter("table_name", LogicalType::VARCHAR)
	    .AddParameter("src_column", LogicalType::VARCHAR)
	    .AddParameter("dst_column", LogicalType::VARCHAR);
	triangle_count.bind_replace = TriangleCountBindReplace;
	set.AddFunction(triangle_count);
}

} // namespace duckdb
