#include "duckdb/function/scalar/variant_utils.hpp"

namespace duckdb {

void VariantUtils::SortVariantKeys(Vector &dictionary, idx_t dictionary_size, SelectionVector &sel, idx_t sel_size) {
	auto &allocator = Allocator::DefaultAllocator();
	auto dictionary_data = FlatVector::GetData<string_t>(dictionary);

	//! string + unsorted_index
	vector<std::pair<reference<string_t>, idx_t>> strings;
	strings.reserve(dictionary_size);
	for (idx_t i = 0; i < dictionary_size; i++) {
		strings.emplace_back(dictionary_data[i], i);
	}

	//! Sort the unique strings
	std::sort(strings.begin(), strings.end(),
	          [](const std::pair<reference<string_t>, idx_t> &a, const std::pair<reference<string_t>, idx_t> &b) {
		          return a.first.get() < b.first.get();
	          });

	bool is_already_sorted = true;
	vector<idx_t> unsorted_to_sorted(strings.size());
	for (idx_t i = 0; i < strings.size(); i++) {
		if (i != strings[i].second) {
			is_already_sorted = false;
		}
		unsorted_to_sorted[strings[i].second] = i;
	}

	if (is_already_sorted) {
		return;
	}

	//! Adjust the selection vector to point to the right dictionary index
	for (idx_t i = 0; i < sel_size; i++) {
		auto old_dictionary_index = sel.get_index(i);
		auto new_dictionary_index = unsorted_to_sorted[old_dictionary_index];
		sel.set_index(i, new_dictionary_index);
	}

	//! Finally, rewrite the dictionary itself
	auto copied_dictionary = allocator.Allocate(sizeof(string_t) * dictionary_size);
	auto copied_dictionary_data = reinterpret_cast<string_t *>(copied_dictionary.get());
	memcpy(copied_dictionary_data, dictionary_data, sizeof(string_t) * dictionary_size);

	for (idx_t i = 0; i < dictionary_size; i++) {
		dictionary_data[i] = copied_dictionary_data[strings[i].second];
	}
}

} // namespace duckdb
