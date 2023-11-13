//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/operation_converter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/column_binding_replacer.hpp"

namespace duckdb {

//! The Operation Converter converts Set operations to joins when possible
class OperationConverter {
public:

	OperationConverter(LogicalOperator &root) : root(root) {
		//! We don't want to resolve execute because execute is assumed to be resolved already.
		//! and the types are deleted if you call resolve types on a logical execute
		//! this is bad, because when you want to add a relation in the join order optimizer,
		//! and the execute doesn't have any types, then no table references are generated.
		//! causing the D_ASSERT in relation_manager.cpp:60 to break;

		//! but we do need to resolve all types so that the types of a projection match the
		//! size of the column bindings.


		//! Just need to know what the logical execute does. So confused.
//		auto &op = root;
//		while (root.type == LogicalOperatorType::LOGICAL_EXECUTE) {
//			root = root.children[0];
//		}

		// What I want to know is why a logical execute is planned and what it is used for exactly.
		// It doesn't make a lot of sense to me.
		root.ResolveOperatorTypes();
	}
	//! Perform DelimJoin elimination
	void Optimize(unique_ptr<LogicalOperator> &op);
	LogicalOperator &root;

private:
};

} // namespace duckdb
