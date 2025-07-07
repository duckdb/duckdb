#include "duckdb/execution/operator/helper/physical_unified_string_dictionary_insertion.h"
#include "duckdb/common/printer.hpp"

namespace duckdb {

class USDInsertionState : public OperatorState {
public:
	explicit USDInsertionState(ExecutionContext &context, idx_t cols) {
		for (idx_t i = 0; i < cols; ++i) {
			current_dict_ids.push_back("");
		}
	}
	// keeps track of unique dictionary IDs per incoming column to make sure each dictionary is inserted once only.
	vector<string> current_dict_ids;
	// statistics from dictionary insertion. We use them to stop inserting if the domain cardinality is too high.
	idx_t n_success = 0;
	idx_t n_already_exists = 0;
	idx_t n_rejected_full = 0;
	idx_t n_rejected_probing = 0;
	idx_t n_invalid = 0;
};

class USDInsertionGState : public GlobalOperatorState {
public:
	explicit USDInsertionGState(ClientContext &context, idx_t cols) {
		for (idx_t i = 0; i < cols; ++i) {
			inserted_strings.push_back(0);
			unique_strings_in_usd_per_column.push_back(0);
			inserted_dictionaries.push_back(0);
			is_high_cardinality.push_back(false);
		}
	}
	mutex statistics_lock;
	vector<bool> is_high_cardinality;
	vector<idx_t> inserted_strings;
	vector<idx_t> unique_strings_in_usd_per_column;
	vector<idx_t> inserted_dictionaries;
};

void PhysicalUnifiedStringDictionary::InsertConstant(ExecutionContext &context, Vector &vec) const {
	auto str_value = reinterpret_cast<string_t *>(ConstantVector::GetData(vec));
	auto validity = ConstantVector::Validity(vec);
	if (validity.AllValid()) {
		context.client.GetUnifiedStringDictionary().Insert(*str_value);
	}
}

void PhysicalUnifiedStringDictionary::InsertFlat(ExecutionContext &context, Vector &vec, idx_t count) const {
	auto start = reinterpret_cast<string_t *>(FlatVector::GetData(vec));
	auto validity = FlatVector::Validity(vec);
	for (idx_t i = 0; i < count; ++i) {
		if (validity.RowIsValid(i)) {
			context.client.GetUnifiedStringDictionary().Insert(start[i]);
		}
	}
}

void PhysicalUnifiedStringDictionary::InsertDictionary(ExecutionContext &context, Vector &vec, OperatorState &state_p,
                                                       GlobalOperatorState &gstate_p, idx_t col_idx) const {
	auto &state = state_p.Cast<USDInsertionState>();
	auto &gstate = gstate_p.Cast<USDInsertionGState>();
	auto &dict = DictionaryVector::Child(vec);
	auto size = DictionaryVector::DictionarySize(vec);
	if (!size.IsValid() || size.GetIndex() > MAX_DICT_SIZE) {
		return;
	}

	if (gstate.is_high_cardinality[col_idx] || DictionaryVector::DictionaryId(vec) == state.current_dict_ids[col_idx]) {
		return;
	}

	auto dict_validity = FlatVector::Validity(dict);
	for (idx_t i = 0; i < size.GetIndex(); ++i) {
		if (!dict_validity.RowIsValid(i)) {
			continue;
		}

		auto res = context.client.GetUnifiedStringDictionary().Insert(reinterpret_cast<string_t *>(dict.GetData())[i]);
		switch (res) {
		case USDInsertResult::SUCCESS:
			++state.n_success;
			break;
		case USDInsertResult::ALREADY_EXISTS:
			++state.n_already_exists;
			break;
		case USDInsertResult::REJECTED_PROBING:
			++state.n_rejected_probing;
			break;
		case USDInsertResult::REJECTED_FULL:
			++state.n_rejected_full;
			break;
		default:
			break;
		}
	}

	UpdateDictionaryState(context, state_p, gstate_p, vec, col_idx, size.GetIndex());
}

void PhysicalUnifiedStringDictionary::UpdateDictionaryState(ExecutionContext &context, OperatorState &state_p,
                                                            GlobalOperatorState &gstate_p, Vector &vec, idx_t col_idx,
                                                            idx_t dict_size) const {
	auto &state = state_p.Cast<USDInsertionState>();
	auto &gstate = gstate_p.Cast<USDInsertionGState>();
	// Update current dictionary ID
	state.current_dict_ids[col_idx] = DictionaryVector::DictionaryId(vec);

	{
		unique_lock<mutex> lock(gstate.statistics_lock);
		gstate.inserted_strings[col_idx] += dict_size;
		gstate.unique_strings_in_usd_per_column[col_idx] += state.n_success;
		gstate.inserted_dictionaries[col_idx]++;

		if (gstate.inserted_dictionaries[col_idx] > MIN_DICTIONARY_SEEN) {
			double avg_growth = static_cast<double>(gstate.unique_strings_in_usd_per_column[col_idx]) /
			                    static_cast<double>(gstate.inserted_strings[col_idx]);
			if (avg_growth > TOTAL_GROWTH_THRESHOLD) {
				gstate.is_high_cardinality[col_idx] = true;
			}
		}
		if (gstate.unique_strings_in_usd_per_column[col_idx] > MAX_STRINGS_PER_COLUMN) {
			gstate.is_high_cardinality[col_idx] = true;
		}
	}

	context.client.GetUnifiedStringDictionary().UpdateFailedAttempts(state.n_rejected_probing + state.n_rejected_full);

	state.n_success = 0;
	state.n_rejected_full = 0;
	state.n_rejected_probing = 0;
	state.n_already_exists = 0;
}

OperatorResultType PhysicalUnifiedStringDictionary::Execute(ExecutionContext &context, DataChunk &input,
                                                            DataChunk &chunk, GlobalOperatorState &gstate_p,
                                                            OperatorState &state_p) const {
	for (idx_t col_idx = 0; col_idx < input.data.size(); ++col_idx) {
		if (input.data[col_idx].GetType() != LogicalType::VARCHAR || !insert_to_usd[col_idx]) {
			continue;
		}
		auto &col = input.data[col_idx];
		switch (col.GetVectorType()) {
		case VectorType::CONSTANT_VECTOR:
			InsertConstant(context, col);
			break;
		case VectorType::FLAT_VECTOR: {
			if (insert_flat_vectors) {
				InsertFlat(context, col, input.size());
			}
			break;
		}
		case VectorType::DICTIONARY_VECTOR: {
			InsertDictionary(context, col, state_p, gstate_p, col_idx);
			break;
		}
		default:
			break;
		}
	}
	chunk.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

unique_ptr<OperatorState> PhysicalUnifiedStringDictionary::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<USDInsertionState>(context, insert_to_usd.size());
}

unique_ptr<GlobalOperatorState> PhysicalUnifiedStringDictionary::GetGlobalOperatorState(ClientContext &context) const {
	return make_uniq<USDInsertionGState>(context, insert_to_usd.size());
}

} // namespace duckdb
