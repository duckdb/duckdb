#include "duckdb/planner/logical_operator.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/serializer/buffered_deserializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/optimizer/cascade/base/CReqdPropRelational.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

namespace duckdb {
const uint64_t PLAN_SERIALIZATION_VERSION = 1;

LogicalOperator::LogicalOperator(LogicalOperatorType type) {
	logical_type = type;
	estimated_cardinality = 0;
	has_estimated_cardinality = false;
}

LogicalOperator::LogicalOperator(LogicalOperatorType type, vector<unique_ptr<Expression>> expressions) {
	logical_type = type;
	this->expressions = std::move(expressions);
	estimated_cardinality = 0;
	has_estimated_cardinality = false;
}

LogicalOperator::~LogicalOperator() {
}

CReqdProp *LogicalOperator::PrpCreate() const {
	return new CReqdPropRelational();
}

vector<ColumnBinding> LogicalOperator::GetColumnBindings() {
	return {ColumnBinding(0, 0)};
}

string LogicalOperator::GetName() const {
	return LogicalOperatorToString(logical_type);
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

vector<ColumnBinding> LogicalOperator::GenerateColumnBindings(idx_t table_idx, idx_t column_count) {
	vector<ColumnBinding> result;
	result.reserve(column_count);
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
}

idx_t LogicalOperator::EstimateCardinality(ClientContext &context) {
	// simple estimator, just take the max of the children
	if (has_estimated_cardinality) {
		return estimated_cardinality;
	}
	idx_t max_cardinality = 0;
	for (auto &child : children) {
		LogicalOperator *logical_child = static_cast<LogicalOperator *>(child.get());
		max_cardinality = MaxValue(logical_child->EstimateCardinality(context), max_cardinality);
	}
	has_estimated_cardinality = true;
	estimated_cardinality = max_cardinality;
	return estimated_cardinality;
}

ULONG LogicalOperator::HashValue() const {
	ULONG oid = (ULONG)logical_type;
	return gpos::HashValue<ULONG>(&oid);
}

void LogicalOperator::Print() {
	Printer::Print(ToString());
}

void LogicalOperator::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteField<LogicalOperatorType>(logical_type);
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
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		result = LogicalAsOfJoin::Deserialize(state, reader);
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
	case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
		result = LogicalPositionalJoin::Deserialize(state, reader);
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
	case LogicalOperatorType::LOGICAL_PRAGMA:
		result = LogicalPragma::Deserialize(state, reader);
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
	case LogicalOperatorType::LOGICAL_SET:
		result = LogicalSet::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_RESET:
		result = LogicalReset::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_ALTER:
	case LogicalOperatorType::LOGICAL_VACUUM:
	case LogicalOperatorType::LOGICAL_LOAD:
	case LogicalOperatorType::LOGICAL_ATTACH:
	case LogicalOperatorType::LOGICAL_TRANSACTION:
	case LogicalOperatorType::LOGICAL_DROP:
		result = LogicalSimple::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_DETACH:
		throw SerializationException("Logical Detach does not support serialization");
	case LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR:
		result = LogicalExtensionOperator::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_PIVOT:
		result = LogicalPivot::Deserialize(state, reader);
		break;
	case LogicalOperatorType::LOGICAL_INVALID:
		/* no default here to trigger a warning if we forget to implement deserialize for a new operator */
		throw SerializationException("Invalid type for operator deserialization");
	}
	reader.Finalize();
	result->children.clear();
	for (auto &child : children) {
		result->children.emplace_back(std::move(child));
	}
	return result;
}

vector<idx_t> LogicalOperator::GetTableIndex() const {
	return {};
}

unique_ptr<LogicalOperator> LogicalOperator::Copy(ClientContext &context) const {
	BufferedSerializer logical_op_serializer;
	try {
		this->Serialize(logical_op_serializer);
	} catch (NotImplementedException &ex) {
		throw NotImplementedException(
		    "Logical Operator Copy requires the logical operator and all of its children to be serializable: " +
		    std::string(ex.what()));
	}
	auto data = logical_op_serializer.GetData();
	auto logical_op_deserializer = BufferedContextDeserializer(context, data.data.get(), data.size);
	PlanDeserializationState state(context);
	auto op_copy = LogicalOperator::Deserialize(logical_op_deserializer, state);
	return op_copy;
}

CDrvdProp *LogicalOperator::PdpCreate() {
	return new CDrvdPropRelational();
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::PkcDeriveKeysPassThru
//
//	@doc:
//		Addref and return keys of n-th child
//
//---------------------------------------------------------------------------
CKeyCollection *LogicalOperator::PkcDeriveKeysPassThru(CExpressionHandle &expression_handle, ULONG ul_child) {
	CKeyCollection *pkc_left = expression_handle.GetRelationalProperties(ul_child)->GetKeyCollection();
	// key collection may be NULL
	return pkc_left;
}

CKeyCollection *LogicalOperator::DeriveKeyCollection(CExpressionHandle &expression_handle) {
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		LogicalOperator::DeriveJoinDepth
//
//	@doc:
//		Derive join depth
//
//---------------------------------------------------------------------------
ULONG LogicalOperator::DeriveJoinDepth(CExpressionHandle &expression_handle) {
	const ULONG arity = expression_handle.Arity();
	// sum-up join depth of all relational children
	ULONG ul_depth = 0;
	for (ULONG ul = 0; ul < arity; ul++) {
		if (!expression_handle.FScalarChild(ul)) {
			ul_depth = ul_depth + expression_handle.DeriveJoinDepth(ul);
		}
	}
	return ul_depth;
}

//---------------------------------------------------------------------------
//	@function:
//		LogicalOperator::PpcDeriveConstraintPassThru
//
//	@doc:
//		Shorthand to addref and pass through constraint from a given child
//
//---------------------------------------------------------------------------
CPropConstraint *LogicalOperator::PpcDeriveConstraintPassThru(CExpressionHandle &expression_handle, ULONG ul_child) {
	// return constraint property of child
	CPropConstraint *ppc = expression_handle.DerivePropertyConstraint(ul_child);
	return ppc;
}

//---------------------------------------------------------------------------
//	@function:
//		LogicalOperator::PpcDeriveConstraintFromPredicates
//
//	@doc:
//		Derive constraint property when expression has relational children and
//		scalar children (predicates)
//
//---------------------------------------------------------------------------
CPropConstraint *LogicalOperator::PpcDeriveConstraintFromPredicates(CExpressionHandle &expression_handle) {
	vector<vector<ColumnBinding>> pdrgpcrs;
	vector<Expression *> pdrgpcnstr;
	// collect constraint properties from relational children
	// and predicates from scalar children
	ULONG arity = expression_handle.Arity(0);
	for (ULONG ul = 0; ul < arity; ul++) {
		CPropConstraint *ppc = expression_handle.DerivePropertyConstraint(ul);
		// equivalence classes coming from child
		vector<vector<ColumnBinding>> pdrgpcrs_child = ppc->PdrgpcrsEquivClasses();
		// merge with the equivalence classes we have so far
		vector<vector<ColumnBinding>> pdrgpcrs_merged = CUtils::PdrgpcrsMergeEquivClasses(pdrgpcrs, pdrgpcrs_child);
		pdrgpcrs = pdrgpcrs_merged;
		// constraint coming from child
		Expression *pcnstr = ppc->Pcnstr();
		if (nullptr != pcnstr) {
			pdrgpcnstr.push_back(pcnstr);
		}
	}
	arity = expression_handle.Arity(1);
	for (ULONG ul = 0; ul < arity; ul++) {
		Expression *expression_scalar = expression_handle.PexprScalarExactChild(ul);
		vector<ColumnBinding> v = expression_scalar->getColumnBinding();
		vector<vector<ColumnBinding>> pdrgpcrs_child;
		pdrgpcrs_child = CUtils::AddEquivClassToArray(v, pdrgpcrs_child);
		if (nullptr != expression_scalar) {
			pdrgpcnstr.push_back(expression_scalar);
			// merge with the equivalence classes we have so far
			vector<vector<ColumnBinding>> pdrgpcrs_merged = CUtils::PdrgpcrsMergeEquivClasses(pdrgpcrs, pdrgpcrs_child);
			pdrgpcrs = pdrgpcrs_merged;
		}
	}
	Expression *pcnstr_new = new BoundConjunctionExpression(ExpressionType::CONJUNCTION_AND);
	return new CPropConstraint(pdrgpcrs, pcnstr_new);
}
} // namespace duckdb