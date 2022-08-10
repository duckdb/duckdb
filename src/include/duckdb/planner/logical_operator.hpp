//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/logical_operator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/column_binding.hpp"

#include <functional>
#include <algorithm>

namespace duckdb {

class FieldWriter;
class FieldReader;

//! The current version of the plan serialization format. Exposed via by @Serializer & @Desializer
//! to be used by various Operator to know what format to read and write.
//! nocommit: some question for discussions:
//!  - what is the right scope for this versioning? Should it be plan specific? should we something that represents
//!    the DuckDb version?
//!  - Tied to the above - we'll need serialize extension specific logic. What happens when there's a change there?
//!    do we expect to have to bump the DuckDb format version? If so, would it be simpler to just bump on every release
//!    (a new version doesn't mean old versions can't be read).
//!  - who should set the version? the version of a stream is typically set by the opener of the stream. For example:
//!		- When reading from a file, the file header will have the version which will be set on the stream
//!     - When reading / writing to a socket (and some other code on the other side), a shared version will be
//! 	  negotiated when opening the socket and used for the rest of the communication.
extern const uint64_t PLAN_SERIALIZATION_VERSION;

//! LogicalOperator is the base class of the logical operators present in the
//! logical query tree
class LogicalOperator {
public:
	explicit LogicalOperator(LogicalOperatorType type);
	LogicalOperator(LogicalOperatorType type, vector<unique_ptr<Expression>> expressions);
	virtual ~LogicalOperator();

	//! The type of the logical operator
	LogicalOperatorType type;
	//! The set of children of the operator
	vector<unique_ptr<LogicalOperator>> children;
	//! The set of expressions contained within the operator, if any
	vector<unique_ptr<Expression>> expressions;
	//! The types returned by this logical operator. Set by calling LogicalOperator::ResolveTypes.
	vector<LogicalType> types;
	//! Estimated Cardinality
	idx_t estimated_cardinality = 0;

public:
	virtual vector<ColumnBinding> GetColumnBindings();
	static vector<ColumnBinding> GenerateColumnBindings(idx_t table_idx, idx_t column_count);
	static vector<LogicalType> MapTypes(const vector<LogicalType> &types, const vector<idx_t> &projection_map);
	static vector<ColumnBinding> MapBindings(const vector<ColumnBinding> &types, const vector<idx_t> &projection_map);

	//! Resolve the types of the logical operator and its children
	void ResolveOperatorTypes();

	virtual string GetName() const;
	virtual string ParamsToString() const;
	virtual string ToString() const;
	DUCKDB_API void Print();
	//! Debug method: verify that the integrity of expressions & child nodes are maintained
	virtual void Verify();

	void AddChild(unique_ptr<LogicalOperator> child);

	virtual idx_t EstimateCardinality(ClientContext &context);

	//! Serializes a LogicalOperator to a stand-alone binary blob
	void Serialize(Serializer &serializer) const;
	//! Serializes an LogicalOperator to a stand-alone binary blob
	virtual void Serialize(FieldWriter &writer) const = 0;

	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer, ClientContext &context);

protected:
	//! Resolve types for this specific operator
	virtual void ResolveTypes() = 0;
};
} // namespace duckdb
