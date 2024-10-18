#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/uhugeint.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/nested_loop_join.hpp"

namespace duckdb {

template <class T, class OP>
static void TemplatedMarkJoin(Vector &left, Vector &right, idx_t lcount, idx_t rcount, bool found_match[]) {
	using MATCH_OP = ComparisonOperationWrapper<OP>;

	UnifiedVectorFormat left_data, right_data;
	left.ToUnifiedFormat(lcount, left_data);
	right.ToUnifiedFormat(rcount, right_data);

	auto ldata = UnifiedVectorFormat::GetData<T>(left_data);
	auto rdata = UnifiedVectorFormat::GetData<T>(right_data);
	for (idx_t i = 0; i < lcount; i++) {
		if (found_match[i]) {
			continue;
		}
		auto lidx = left_data.sel->get_index(i);
		const auto left_null = !left_data.validity.RowIsValid(lidx);
		if (!MATCH_OP::COMPARE_NULL && left_null) {
			continue;
		}
		for (idx_t j = 0; j < rcount; j++) {
			auto ridx = right_data.sel->get_index(j);
			const auto right_null = !right_data.validity.RowIsValid(ridx);
			if (!MATCH_OP::COMPARE_NULL && right_null) {
				continue;
			}
			if (MATCH_OP::template Operation<T>(ldata[lidx], rdata[ridx], left_null, right_null)) {
				found_match[i] = true;
				break;
			}
		}
	}
}

static void MarkJoinNested(Vector &left, Vector &right, idx_t lcount, idx_t rcount, bool found_match[],
                           ExpressionType comparison_type) {
	Vector left_reference(left.GetType());
	for (idx_t i = 0; i < lcount; i++) {
		if (found_match[i]) {
			continue;
		}
		ConstantVector::Reference(left_reference, left, i, rcount);
		idx_t count;
		switch (comparison_type) {
		case ExpressionType::COMPARE_EQUAL:
			count = VectorOperations::Equals(left_reference, right, nullptr, rcount, nullptr, nullptr);
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
			count = VectorOperations::NotEquals(left_reference, right, nullptr, rcount, nullptr, nullptr);
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			count = VectorOperations::LessThan(left_reference, right, nullptr, rcount, nullptr, nullptr);
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			count = VectorOperations::GreaterThan(left_reference, right, nullptr, rcount, nullptr, nullptr);
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			count = VectorOperations::LessThanEquals(left_reference, right, nullptr, rcount, nullptr, nullptr);
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			count = VectorOperations::GreaterThanEquals(left_reference, right, nullptr, rcount, nullptr, nullptr);
			break;
		case ExpressionType::COMPARE_DISTINCT_FROM:
			count = VectorOperations::DistinctFrom(left_reference, right, nullptr, rcount, nullptr, nullptr);
			break;
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
			count = VectorOperations::NotDistinctFrom(left_reference, right, nullptr, rcount, nullptr, nullptr);
			break;
		default:
			throw InternalException("Unsupported comparison type for MarkJoinNested");
		}
		if (count > 0) {
			found_match[i] = true;
		}
	}
}

template <class OP>
static void MarkJoinSwitch(Vector &left, Vector &right, idx_t lcount, idx_t rcount, bool found_match[]) {
	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TemplatedMarkJoin<int8_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::INT16:
		return TemplatedMarkJoin<int16_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::INT32:
		return TemplatedMarkJoin<int32_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::INT64:
		return TemplatedMarkJoin<int64_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::INT128:
		return TemplatedMarkJoin<hugeint_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::UINT8:
		return TemplatedMarkJoin<uint8_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::UINT16:
		return TemplatedMarkJoin<uint16_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::UINT32:
		return TemplatedMarkJoin<uint32_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::UINT64:
		return TemplatedMarkJoin<uint64_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::UINT128:
		return TemplatedMarkJoin<uhugeint_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::FLOAT:
		return TemplatedMarkJoin<float, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::DOUBLE:
		return TemplatedMarkJoin<double, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::VARCHAR:
		return TemplatedMarkJoin<string_t, OP>(left, right, lcount, rcount, found_match);
	default:
		throw NotImplementedException("Unimplemented type for mark join!");
	}
}

static void MarkJoinComparisonSwitch(Vector &left, Vector &right, idx_t lcount, idx_t rcount, bool found_match[],
                                     ExpressionType comparison_type) {
	switch (left.GetType().InternalType()) {
	case PhysicalType::STRUCT:
	case PhysicalType::LIST:
	case PhysicalType::ARRAY:
		return MarkJoinNested(left, right, lcount, rcount, found_match, comparison_type);
	default:
		break;
	}
	D_ASSERT(left.GetType() == right.GetType());
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return MarkJoinSwitch<Equals>(left, right, lcount, rcount, found_match);
	case ExpressionType::COMPARE_NOTEQUAL:
		return MarkJoinSwitch<NotEquals>(left, right, lcount, rcount, found_match);
	case ExpressionType::COMPARE_LESSTHAN:
		return MarkJoinSwitch<LessThan>(left, right, lcount, rcount, found_match);
	case ExpressionType::COMPARE_GREATERTHAN:
		return MarkJoinSwitch<GreaterThan>(left, right, lcount, rcount, found_match);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return MarkJoinSwitch<LessThanEquals>(left, right, lcount, rcount, found_match);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return MarkJoinSwitch<GreaterThanEquals>(left, right, lcount, rcount, found_match);
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return MarkJoinSwitch<DistinctFrom>(left, right, lcount, rcount, found_match);
	default:
		throw NotImplementedException("Unimplemented comparison type for join!");
	}
}

void NestedLoopJoinMark::Perform(DataChunk &left, ColumnDataCollection &right, bool found_match[],
                                 const vector<JoinCondition> &conditions) {
	// initialize a new temporary selection vector for the left chunk
	// loop over all chunks in the RHS
	ColumnDataScanState scan_state;
	right.InitializeScan(scan_state);

	DataChunk scan_chunk;
	right.InitializeScanChunk(scan_chunk);

	while (right.Scan(scan_state, scan_chunk)) {
		for (idx_t i = 0; i < conditions.size(); i++) {
			MarkJoinComparisonSwitch(left.data[i], scan_chunk.data[i], left.size(), scan_chunk.size(), found_match,
			                         conditions[i].comparison);
		}
	}
}

} // namespace duckdb
