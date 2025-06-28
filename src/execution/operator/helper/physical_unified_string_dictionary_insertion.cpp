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
	vector<string> current_dict_ids;
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
			inserted_unique_strings.push_back(0);
			unique_strings_in_unified_dictionary_per_column.push_back(0);
			inserted_dictionaries.push_back(0);
			is_high_cardinality.push_back(false);
		}
	}
	mutex statistics_lock;
	vector<bool> is_high_cardinality;
	vector<idx_t> inserted_unique_strings;
	vector<idx_t> unique_strings_in_unified_dictionary_per_column;
	vector<idx_t> inserted_dictionaries;
};

OperatorResultType PhysicalUnifiedStringDictionary::Execute(ExecutionContext &context, DataChunk &input,
                                                            DataChunk &chunk, GlobalOperatorState &gstate,
                                                            OperatorState &state_p) const {
	auto &state = state_p.Cast<USDInsertionState>();
	auto &global_state = gstate.Cast<USDInsertionGState>();
	for (idx_t col_idx = 0; col_idx < input.data.size(); ++col_idx) {
		if (input.data[col_idx].GetType() != LogicalType::VARCHAR || !insert_to_usd[col_idx] ||
		    input.size() == 1 // FIXME: this condition is not needed but there's an extremely odd bug with this test:
		                      // test/sql/copy/partitioned/hive_partition_escape.test
		) {
			continue;
		}

		if (input.data[col_idx].GetVectorType() == VectorType::CONSTANT_VECTOR) {
			auto str_value = reinterpret_cast<string_t *>(ConstantVector::GetData(input.data[col_idx]));
			auto validity = ConstantVector::Validity(input.data[col_idx]);
			if (validity.AllValid()) {
				context.client.GetUnifiedStringDictionary().Insert(*str_value);
			}
		} else if (input.data[col_idx].GetVectorType() == VectorType::FLAT_VECTOR && insert_flat_vectors) {
			auto start = reinterpret_cast<string_t *>(FlatVector::GetData(input.data[col_idx]));
			auto validity = FlatVector::Validity(input.data[col_idx]);
			for (idx_t i = 0; i < input.size(); i++) {
				if (validity.RowIsValid(i)) {
					context.client.GetUnifiedStringDictionary().Insert(start[i]);
				}
			}
		} else if (input.data[col_idx].GetVectorType() == VectorType::DICTIONARY_VECTOR) {
			auto &dict = DictionaryVector::Child(input.data[col_idx]);
			auto size = DictionaryVector::DictionarySize(input.data[col_idx]);
			if (!size.IsValid()) {
				continue;
			}
			auto dict_validity = FlatVector::Validity(dict);

			if (!global_state.is_high_cardinality[col_idx] &&
			    DictionaryVector::DictionaryId(input.data[col_idx]) != state.current_dict_ids[col_idx]) {
				auto start = reinterpret_cast<string_t *>(dict.GetData());
				for (idx_t i = 0; i < size.GetIndex(); i++) {
					if (!dict_validity.RowIsValid(i)) {
						continue;
					}
					auto result = context.client.GetUnifiedStringDictionary().Insert(start[i]);
					// process the results, we use the statistics to determine the unique cardinality of the column
					switch (result) {
					case InsertResult::SUCCESS:
						++state.n_success;
						break;
					case InsertResult::ALREADY_EXISTS:
						++state.n_already_exists;
						break;
					case InsertResult::REJECTED_PROBING:
						++state.n_rejected_probing;
						break;
					case InsertResult::REJECTED_FULL:
						++state.n_rejected_full;
						break;
					default:
						break;
					}
				}
				// update local and global states
				state.current_dict_ids[col_idx] = DictionaryVector::DictionaryId(input.data[col_idx]);
				unique_lock<mutex> lock(global_state.statistics_lock);
				global_state.inserted_unique_strings[col_idx] += size.GetIndex();
				global_state.unique_strings_in_unified_dictionary_per_column[col_idx] += state.n_success;
				global_state.inserted_dictionaries[col_idx]++;

				constexpr double TOTAL_GROWTH_THRESHOLD = 0.1;
				const idx_t MIN_DICTIONARY_SEEN = 10;
				constexpr idx_t HARD_LIMIT = 100000;

				if (global_state.inserted_dictionaries[col_idx] > MIN_DICTIONARY_SEEN) {
					auto avg_growth =
					    static_cast<double>(global_state.unique_strings_in_unified_dictionary_per_column[col_idx]) /
					    static_cast<double>(global_state.inserted_unique_strings[col_idx]);

					if (avg_growth > TOTAL_GROWTH_THRESHOLD) {
						global_state.is_high_cardinality[col_idx] = true;
					}
				}
				if (global_state.unique_strings_in_unified_dictionary_per_column[col_idx] > HARD_LIMIT) {
					global_state.is_high_cardinality[col_idx] = true;
				}
				lock.unlock();

				context.client.GetUnifiedStringDictionary().UpdateFailedAttempts(state.n_rejected_probing +
				                                                                 state.n_rejected_full);

				state.n_success = 0;
				state.n_rejected_full = 0;
				state.n_rejected_probing = 0;
				state.n_already_exists = 0;
			}
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
