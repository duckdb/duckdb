//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_shared_expressions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/expression_map.hpp"

namespace duckdb {

class ExpressionExecutor;
class DataChunk;

//! A shared set of expressions
struct WindowSharedExpressions {
	struct Shared {
		column_t size = 0;
		expression_map_t<vector<column_t>> columns;
	};

	//! Register a shared expression in a shared set
	static column_t RegisterExpr(const unique_ptr<Expression> &expr, Shared &shared);

	//! Register a shared collection expression
	column_t RegisterCollection(const unique_ptr<Expression> &expr, bool build_validity) {
		auto result = RegisterExpr(expr, coll_shared);
		if (build_validity) {
			coll_validity.insert(result);
		}
		return result;
	}
	//! Register a shared collection expression
	inline column_t RegisterSink(const unique_ptr<Expression> &expr) {
		return RegisterExpr(expr, sink_shared);
	}
	//! Register a shared evaluation expression
	inline column_t RegisterEvaluate(const unique_ptr<Expression> &expr) {
		return RegisterExpr(expr, eval_shared);
	}

	//! Expression layout
	static vector<optional_ptr<const Expression>> GetSortedExpressions(Shared &shared);

	//! Expression execution utility
	static void PrepareExecutors(Shared &shared, ExpressionExecutor &exec, DataChunk &chunk);

	//! Prepare collection expressions
	inline void PrepareCollection(ExpressionExecutor &exec, DataChunk &chunk) {
		PrepareExecutors(coll_shared, exec, chunk);
	}

	//! Prepare collection expressions
	inline void PrepareSink(ExpressionExecutor &exec, DataChunk &chunk) {
		PrepareExecutors(sink_shared, exec, chunk);
	}

	//! Prepare collection expressions
	inline void PrepareEvaluate(ExpressionExecutor &exec, DataChunk &chunk) {
		PrepareExecutors(eval_shared, exec, chunk);
	}

	//! Fully materialised shared expressions
	Shared coll_shared;
	//! Sink shared expressions
	Shared sink_shared;
	//! Evaluate shared expressions
	Shared eval_shared;
	//! Requested collection validity masks
	unordered_set<column_t> coll_validity;
};

} // namespace duckdb
