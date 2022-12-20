#include "duckdb/planner/logical_operator.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/serializer/buffered_deserializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

namespace duckdb {

const uint64_t PLAN_SERIALIZATION_VERSION = 1;

LogicalOperator::LogicalOperator(LogicalOperatorType type)
    : type(type), estimated_cardinality(0), has_estimated_cardinality(false) {
}

LogicalOperator::LogicalOperator(LogicalOperatorType type, vector<unique_ptr<Expression>> expressions)
    : type(type), expressions(move(expressions)), estimated_cardinality(0), has_estimated_cardinality(false) {
}

LogicalOperator::~LogicalOperator() {
}

vector<ColumnBinding> LogicalOperator::GetColumnBindings() {
	return {ColumnBinding(0, 0)};
}

string LogicalOperator::GetName() const {
	return LogicalOperatorToString(type);
}

string LogicalOperator::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < expressions.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += expressions[i]->GetName();
	}
	return result;
}

void LogicalOperator::ResolveOperatorTypes() {

	types.clear();
	// first resolve child types
	for (auto &child : children) {
		child->ResolveOperatorTypes();
	}
	// now resolve the types for this operator
	ResolveTypes();
	D_ASSERT(types.size() == GetColumnBindings().size());
}

vector<ColumnBinding> LogicalOperator::GenerateColumnBindings(idx_t table_idx, idx_t column_count) {
	vector<ColumnBinding> result;
	for (idx_t i = 0; i < column_count; i++) {
		result.emplace_back(table_idx, i);
	}
	return result;
}

vector<LogicalType> LogicalOperator::MapTypes(const vector<LogicalType> &types, const vector<idx_t> &projection_map) {
	if (projection_map.empty()) {
		return types;
	} else {
		vector<LogicalType> result_types;
		result_types.reserve(projection_map.size());
		for (auto index : projection_map) {
			result_types.push_back(types[index]);
		}
		return result_types;
	}
}

vector<ColumnBinding> LogicalOperator::MapBindings(const vector<ColumnBinding> &bindings,
                                                   const vector<idx_t> &projection_map) {
	if (projection_map.empty()) {
		return bindings;
	} else {
		vector<ColumnBinding> result_bindings;
		result_bindings.reserve(projection_map.size());
		for (auto index : projection_map) {
			result_bindings.push_back(bindings[index]);
		}
		return result_bindings;
	}
}

string LogicalOperator::ToString() const {
	TreeRenderer renderer;
	return renderer.ToString(*this);
}

void LogicalOperator::Verify(ClientContext &context) {
#ifdef DEBUG
	// verify expressions
	for (idx_t expr_idx = 0; expr_idx < expressions.size(); expr_idx++) {
		auto str = expressions[expr_idx]->ToString();
		// verify that we can (correctly) copy this expression
		auto copy = expressions[expr_idx]->Copy();
		auto original_hash = expressions[expr_idx]->Hash();
		auto copy_hash = copy->Hash();
		// copy should be identical to original
		D_ASSERT(expressions[expr_idx]->ToString() == copy->ToString());
		D_ASSERT(original_hash == copy_hash);
		D_ASSERT(Expression::Equals(expressions[expr_idx].get(), copy.get()));

		D_ASSERT(!Expression::Equals(expressions[expr_idx].get(), nullptr));
		for (idx_t other_idx = 0; other_idx < expr_idx; other_idx++) {
			// comparison with other expressions
			auto other_hash = expressions[other_idx]->Hash();
			bool expr_equal = Expression::Equals(expressions[expr_idx].get(), expressions[other_idx].get());
			if (original_hash != other_hash) {
				// if the hashes are not equal the expressions should not be equal either
				D_ASSERT(!expr_equal);
			}
		}
		D_ASSERT(!str.empty());

		// verify that serialization + deserialization round-trips correctly
		if (expressions[expr_idx]->HasParameter()) {
			continue;
		}
		BufferedSerializer serializer;
		try {
			expressions[expr_idx]->Serialize(serializer);
		} catch (NotImplementedException &ex) {
			// ignore for now (FIXME)
			return;
		}

		auto data = serializer.GetData();
		auto deserializer = BufferedDeserializer(data.data.get(), data.size);

		PlanDeserializationState state(context);
		auto deserialized_expression = Expression::Deserialize(deserializer, state);
		// FIXME: expressions might not be equal yet because of statistics propagation
		continue;
		D_ASSERT(Expression::Equals(expressions[expr_idx].get(), deserialized_expression.get()));
		D_ASSERT(expressions[expr_idx]->Hash() == deserialized_expression->Hash());
	}
	D_ASSERT(!ToString().empty());
	for (auto &child : children) {
		child->Verify(context);
	}
#endif
}

void LogicalOperator::AddChild(unique_ptr<LogicalOperator> child) {
	D_ASSERT(child);
	children.push_back(move(child));
}

idx_t LogicalOperator::EstimateCardinality(ClientContext &context) {
	// simple estimator, just take the max of the children
	if (has_estimated_cardinality) {
		return estimated_cardinality;
	}
	idx_t max_cardinality = 0;
	for (auto &child : children) {
		max_cardinality = MaxValue(child->EstimateCardinality(context), max_cardinality);
	}
	has_estimated_cardinality = true;
	return max_cardinality;
}

void LogicalOperator::Print() {
	Printer::Print(ToString());
}

void LogicalOperator::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteField<LogicalOperatorType>(type);
	writer.WriteSerializableList(children);

	Serialize(writer);
	writer.Finalize();
}

unique_ptr<LogicalOperator> LogicalOperator::Deserialize(Deserializer &deserializer, PlanDeserializationState &gstate) {
	unique_ptr<LogicalOperator> result;

	FieldReader reader(deserializer);
	auto type = reader.ReadRequired<LogicalOperatorType>();
	auto children = reader.ReadRequiredSerializableList<LogicalOperator>(gstate);

	LogicalDeserializationState state(gstate, type, children);
	switch (type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
		result = LogicalProjection::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_FILTER:
		result = LogicalFilter::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		result = LogicalAggregate::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_WINDOW:
		result = LogicalWindow::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_UNNEST:
		result = LogicalUnnest::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_LIMIT:
		result = LogicalLimit::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		result = LogicalOrder::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_TOP_N:
		result = LogicalTopN::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_COPY_TO_FILE:
		result = LogicalCopyToFile::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_DISTINCT:
		result = LogicalDistinct::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_SAMPLE:
		result = LogicalSample::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_LIMIT_PERCENT:
		result = LogicalLimitPercent::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_GET:
		result = LogicalGet::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
		result = LogicalColumnDataGet::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_DELIM_GET:
		result = LogicalDelimGet::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
		result = LogicalExpressionGet::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		result = LogicalDummyScan::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		result = LogicalEmptyResult::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_CTE_REF:
		result = LogicalCTERef::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_JOIN:
		throw InternalException("LogicalJoin deserialize not supported");
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		result = LogicalDelimJoin::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		result = LogicalComparisonJoin::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
		result = LogicalAnyJoin::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		result = LogicalCrossProduct::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_UNION:
		result = LogicalSetOperation::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_EXCEPT:
		result = LogicalSetOperation::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_INTERSECT:
		result = LogicalSetOperation::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
		result = LogicalRecursiveCTE::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_INSERT:
		result = LogicalInsert::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_DELETE:
		result = LogicalDelete::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_UPDATE:
		result = LogicalUpdate::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_ALTER:
		result = LogicalSimple::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_CREATE_TABLE:
		result = LogicalCreateTable::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_CREATE_INDEX:
		result = LogicalCreateIndex::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_CREATE_SEQUENCE:
		result = LogicalCreate::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_CREATE_VIEW:
		result = LogicalCreate::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_CREATE_SCHEMA:
		result = LogicalCreate::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_CREATE_MACRO:
		result = LogicalCreate::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_DROP:
		result = LogicalSimple::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_PRAGMA:
		result = LogicalPragma::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_TRANSACTION:
		result = LogicalSimple::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_CREATE_TYPE:
		result = LogicalCreate::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_EXPLAIN:
		result = LogicalExplain::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_SHOW:
		result = LogicalShow::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_PREPARE:
		result = LogicalPrepare::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_EXECUTE:
		result = LogicalExecute::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_EXPORT:
		result = LogicalExport::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_VACUUM:
		result = LogicalSimple::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_SET:
		result = LogicalSet::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_RESET:
		result = LogicalReset::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_LOAD:
		result = LogicalSimple::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR:
		result = LogicalExtensionOperator::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_INVALID:
		/* no default here to trigger a warning if we forget to implement deserialize for a new operator */
		throw SerializationException("Invalid type for operator deserialization");
	}

	reader.Finalize();
	result->children = move(children);

	return result;
}

vector<idx_t> LogicalOperator::GetTableIndex() const {
	return vector<idx_t> {};
}

unique_ptr<LogicalOperator> LogicalOperator::Copy(ClientContext &context) const {
	BufferedSerializer logical_op_serializer;
	try {
		this->Serialize(logical_op_serializer);
	} catch (NotImplementedException &ex) {
		throw NotImplementedException("Logical Operator Copy requires the logical operator and all of its children to "
		                              "be serializable: " +
		                              std::string(ex.what()));
	}
	auto data = logical_op_serializer.GetData();
	auto logical_op_deserializer = BufferedDeserializer(data.data.get(), data.size);
	PlanDeserializationState state(context);
	auto op_copy = LogicalOperator::Deserialize(logical_op_deserializer, state);
	return op_copy;
}

} // namespace duckdb
