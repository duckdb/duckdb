#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

namespace duckdb {

PhysicalComparisonJoin::PhysicalComparisonJoin(LogicalOperator &op, PhysicalOperatorType type,
                                               vector<JoinCondition> conditions_p, JoinType join_type,
                                               idx_t estimated_cardinality)
    : PhysicalJoin(op, type, join_type, estimated_cardinality) {
	conditions.resize(conditions_p.size());
	// we reorder conditions so the ones with COMPARE_EQUAL occur first
	idx_t equal_position = 0;
	idx_t other_position = conditions_p.size() - 1;
	for (idx_t i = 0; i < conditions_p.size(); i++) {
		if (conditions_p[i].comparison == ExpressionType::COMPARE_EQUAL ||
		    conditions_p[i].comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			// COMPARE_EQUAL and COMPARE_NOT_DISTINCT_FROM, move to the start
			conditions[equal_position++] = std::move(conditions_p[i]);
		} else {
			// other expression, move to the end
			conditions[other_position--] = std::move(conditions_p[i]);
		}
	}
}

string PhysicalComparisonJoin::ParamsToString() const {
	string extra_info = JoinTypeToString(join_type) + "\n";
	for (auto &it : conditions) {
		string op = ExpressionTypeToOperator(it.comparison);
		extra_info += it.left->GetName() + " " + op + " " + it.right->GetName() + "\n";
	}
	extra_info += "\n[INFOSEPARATOR]\n";
	extra_info += StringUtil::Format("EC: %llu\n", estimated_props->GetCardinality<idx_t>());
	extra_info += StringUtil::Format("Cost: %llu", (idx_t)estimated_props->GetCost());
	return extra_info;
}

void PhysicalComparisonJoin::ConstructEmptyJoinResult(JoinType join_type, bool has_null, DataChunk &input,
                                                      DataChunk &result) {
	// empty hash table, special case
	if (join_type == JoinType::ANTI) {
		// anti join with empty hash table, NOP join
		// return the input
		D_ASSERT(input.ColumnCount() == result.ColumnCount());
		result.Reference(input);
	} else if (join_type == JoinType::MARK) {
		// MARK join with empty hash table
		D_ASSERT(join_type == JoinType::MARK);
		D_ASSERT(result.ColumnCount() == input.ColumnCount() + 1);
		auto &result_vector = result.data.back();
		D_ASSERT(result_vector.GetType() == LogicalType::BOOLEAN);
		// for every data vector, we just reference the child chunk
		result.SetCardinality(input);
		for (idx_t i = 0; i < input.ColumnCount(); i++) {
			result.data[i].Reference(input.data[i]);
		}
		// for the MARK vector:
		// if the HT has no NULL values (i.e. empty result set), return a vector that has false for every input
		// entry if the HT has NULL values (i.e. result set had values, but all were NULL), return a vector that
		// has NULL for every input entry
		if (!has_null) {
			auto bool_result = FlatVector::GetData<bool>(result_vector);
			for (idx_t i = 0; i < result.size(); i++) {
				bool_result[i] = false;
			}
		} else {
			FlatVector::Validity(result_vector).SetAllInvalid(result.size());
		}
	} else if (join_type == JoinType::LEFT || join_type == JoinType::OUTER || join_type == JoinType::SINGLE) {
		// LEFT/FULL OUTER/SINGLE join and build side is empty
		// for the LHS we reference the data
		result.SetCardinality(input.size());
		for (idx_t i = 0; i < input.ColumnCount(); i++) {
			result.data[i].Reference(input.data[i]);
		}
		// for the RHS
		for (idx_t k = input.ColumnCount(); k < result.ColumnCount(); k++) {
			result.data[k].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result.data[k], true);
		}
	}
}

vector<ColumnBinding> PhysicalComparisonJoin::PcrsRequired(CExpressionHandle &exprhdl, vector<ColumnBinding> pcrs_required,
	                                ULONG child_index, vector<CDrvdProp *> pdrgpdpCtxt, ULONG ulOptReq) {
	vector<ColumnBinding> pcrs_join;
	for (auto &child : this->conditions) {
		vector<ColumnBinding> left_cell = child.left->getColumnBinding();
		pcrs_join.insert(pcrs_join.end(), left_cell.begin(), left_cell.end());
		vector<ColumnBinding> right_cell = child.right->getColumnBinding();
		pcrs_join.insert(pcrs_join.end(), right_cell.begin(), right_cell.end());
	}
	/* Union of join condition cols and required output cols */
	for(auto &child : pcrs_required) {
		bool FAdd = true;
		for(auto &subchild : pcrs_join) {
			if(child == subchild) {
				FAdd = false;
				break;
			}
		}
		if(FAdd) {
			pcrs_join.push_back(child);
		}
	}
	vector<ColumnBinding> pcrs_child_reqd = PcrsChildReqd(exprhdl, pcrs_join, child_index);
	return pcrs_child_reqd;
}

void PhysicalComparisonJoin::CE() {
	if(this->has_estimated_cardinality) {
		return;
	}
	if(!this->children[0]->has_estimated_cardinality) {
		this->children[0]->CE();
	}
	if(this->children[1]->has_estimated_cardinality) {
		this->children[1]->CE();
	}
	this->has_estimated_cardinality = true;
	this->estimated_cardinality = 0.25 * this->children[0]->estimated_cardinality * this->children[1]->estimated_cardinality;
	return;
}
} // namespace duckdb
