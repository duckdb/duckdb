#include "duckdb/execution/operator/helper/physical_ussr_insertion.h"
#include "duckdb/common/printer.hpp"

namespace duckdb {

class USSRInsertionState : public OperatorState {
public:
	explicit USSRInsertionState(ExecutionContext &context, idx_t cols) {
		for (idx_t i = 0; i < cols; ++i) {
			current_dict_ids.push_back("");
			inserted.push_back({});
			count.push_back({});
			analysis_budget.push_back(0);
			current_analysis_count.push_back(0);
		}
	}
	unordered_set<string> strs;
	vector<vector<uint8_t>> inserted;
	vector<vector<uint16_t>> count;
	vector<string> current_dict_ids;
	vector<idx_t> analysis_budget;
	vector<idx_t> current_analysis_count;
};

class USSRInsertionGState : public GlobalOperatorState {
public:
	explicit USSRInsertionGState(ClientContext &context) {
		analyzing_budget = 100;
	}

	mutex budget_lock;
	idx_t analyzing_budget;
	vector<optional_ptr<DataChunk>> cached;
};

OperatorResultType PhysicalUnifiedString::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                  GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<USSRInsertionState>();
	//	auto &gstateussr = gstate.Cast<USSRInsertionGState>();
	for (idx_t col_idx = 0; col_idx < input.data.size(); ++col_idx) {
		if (input.data[col_idx].GetVectorType() != VectorType::DICTIONARY_VECTOR ||
		    input.data[col_idx].GetType() != LogicalType::VARCHAR) {
			continue;
		}
		// no selection vector analysis
		auto &dict = DictionaryVector::Child(input.data[col_idx]);
		auto size = DictionaryVector::DictionarySize(input.data[col_idx]);
//		auto &dict_id = DictionaryVector::DictionaryId(input.data[col_idx]);
		if (!size.IsValid()) {
			continue;
		}
		auto dict_validity = FlatVector::Validity(dict);
//		Printer::Print(dict_validity.ToString());
		bool isParquet;
		if(DictionaryVector::DictionaryId(input.data[col_idx])[0] == 'x'){
			continue;
		} else{
			isParquet = false;
		}

		if (size.GetIndex() <= 500000000) {
			if (insert_to_ussr[col_idx] &&
			    DictionaryVector::DictionaryId(input.data[col_idx]) != state.current_dict_ids[col_idx]) {
				state.current_dict_ids[col_idx] = DictionaryVector::DictionaryId(input.data[col_idx]);
				USSR_insertion_loop(dict.GetData(), size.GetIndex(), context.client,dict_validity, {}, isParquet, false);
			}
		} else {
			if (insert_to_ussr[col_idx] &&
			    DictionaryVector::DictionaryId(input.data[col_idx]) != state.current_dict_ids[col_idx]) {

				// initialize the local state
				state.inserted[col_idx].clear();
				state.count[col_idx].clear();
				state.inserted[col_idx].reserve(size.GetIndex());
				state.count[col_idx].reserve(size.GetIndex());
				for (idx_t i = 0; i < size.GetIndex(); i++) {
					state.inserted[col_idx].push_back(false);
					state.count[col_idx].push_back(0);
				}
				state.analysis_budget[col_idx] = 5;
				state.current_analysis_count[col_idx] = 1;
				auto &sel = DictionaryVector::SelVector(input.data[col_idx]);
				for (idx_t i = 0; i < input.size(); i++) {
					state.count[col_idx][sel.data()[i]]++;
				}

				vector<idx_t> priority_selection;
				for (idx_t i = 1; i < state.count[col_idx].size(); i++) {
					if (state.count[col_idx][i] > (25 * state.current_analysis_count[col_idx])) {
						priority_selection.push_back(i);
						state.inserted[col_idx][i] = true;
					}
				}

				state.current_dict_ids[col_idx] = DictionaryVector::DictionaryId(input.data[col_idx]);
				USSR_insertion_loop(dict.GetData(), size.GetIndex(), context.client, dict_validity, priority_selection, isParquet, true);
			} else if (insert_to_ussr[col_idx] &&
			           DictionaryVector::DictionaryId(input.data[col_idx]) == state.current_dict_ids[col_idx] &&
			           state.current_analysis_count[col_idx] <= state.analysis_budget[col_idx]) {
				state.current_analysis_count[col_idx]++;
				//				Printer::Print(to_string(++vec_counter));

				auto &sel = DictionaryVector::SelVector(input.data[col_idx]);
				for (idx_t i = 0; i < input.size(); i++) {
					state.count[col_idx][sel.data()[i]]++;
				}

				vector<idx_t> priority_selection;
				for (idx_t i = 1; i < state.count[col_idx].size(); ++i) {
					if (state.count[col_idx][i] > (27 * state.current_analysis_count[col_idx]) &&
					    !state.inserted[col_idx][i]) {
						priority_selection.push_back(i);
						state.inserted[col_idx][i] = true;
					}
				}
				USSR_insertion_loop(dict.GetData(), size.GetIndex(), context.client,dict_validity, priority_selection, isParquet, true);
			}
		}
	}
	chunk.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

unique_ptr<OperatorState> PhysicalUnifiedString::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<USSRInsertionState>(context, insert_to_ussr.size());
}

unique_ptr<GlobalOperatorState> PhysicalUnifiedString::GetGlobalOperatorState(ClientContext &context) const {
	return make_uniq<USSRInsertionGState>(context);
}

void PhysicalUnifiedString::USSR_insertion_loop(data_ptr_t dict_strings, idx_t count, ClientContext &context, ValidityMask& validity,
                                                const vector<idx_t> &priority_insertion, bool isParquet, bool exists_prio) const {
	auto start = reinterpret_cast<string_t *>(dict_strings);
	if (priority_insertion.empty() && !exists_prio) {
		for (idx_t i = 0; i < count; i++) {
			if (!validity.RowIsValid(i)) {
				continue;
			}
			start[i] = context.GetCurrentQueryUssr().insert(start[i]);
		}
	} else {
		for (auto string_idx : priority_insertion) {
			if (!validity.RowIsValid(string_idx)) {
				continue;
			}
			start[string_idx] = context.GetCurrentQueryUssr().insert(start[string_idx]);
		}
	}

}

} // namespace duckdb
