#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/function/scalar/regexp.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_size.hpp"
#include "utf8proc.hpp"

namespace duckdb {

struct Iterator {
public:
	Iterator(const size_t size) : size(size) {}
	virtual ~Iterator() {}
	virtual idx_t Next(const char *input) { return 0; }
	bool HasNext() {
		return offset < size;
	}
	idx_t Start() { return start; }
	const size_t size;
protected:
	idx_t start = 0;	// end of last place a delim match was found
	idx_t offset = 0;	// current position
};

struct AsciiIterator : virtual public Iterator {
public:
	AsciiIterator(size_t size, const char *delim, const size_t delim_size) : Iterator(size), delim(delim), delim_size(delim_size) {}
	idx_t Next(const char *input) override {
		// special case: separate by empty delimiter
		if (delim_size == 0) {
			offset++;
			start = offset;
			return offset;
		}
		for (offset = start; HasNext(); offset++) {
			// potential delimiter match
			if (input[offset] == delim[0] && offset + delim_size <= size) {
				idx_t i = 1;
				while (i < delim_size) {
					if (input[offset + i] == delim[i]) {
						i++;
					} else {
						break;
					}
				}
				// delimiter found: skip start over delimiter
				if (i == delim_size) {
					start = offset + delim_size;
					return offset;
				}
			}
		}
		return offset;
	}

protected:
	const char *delim;
	const size_t delim_size;
};

struct UnicodeIterator : virtual public Iterator {
public:
	UnicodeIterator(size_t input_size, const char *delim, const size_t delim_size) : Iterator(input_size), delim_size(delim_size) {
		int cp_sz;
		for (idx_t i = 0; i < delim_size; i += cp_sz) {
			delim_cps.push_back(utf8proc_codepoint(delim, cp_sz));
		}
	}
	idx_t Next(const char *input) override {
		offset = start;
		// special case: separate by empty delimiter
		if (delim_size == 0) {
			offset = utf8proc_next_grapheme(input, size, offset);
			start = offset;
			return offset;
		}
		int cp_sz;
		for (offset = start; HasNext(); offset = utf8proc_next_grapheme(input, size, offset)) {
			// potential delimiter match
			if (utf8proc_codepoint(&input[offset], cp_sz) == delim_cps[0] && offset + delim_size <= size) {
				idx_t delim_offset = cp_sz;
				idx_t i = 1;
				while (delim_offset < delim_size) {
					if (utf8proc_codepoint(&input[offset + delim_offset], cp_sz) == delim_cps[i] || delim_size == 0) {
						delim_offset += cp_sz;
						i++;
					} else {
						break;
					}
				}
				// delimiter found: skip start over delimiter
				if (delim_offset == delim_size) {
					start = offset + delim_size;
					return offset;
				}
			} 
		}
		return offset;
	}
protected:
	vector<utf8proc_int32_t> delim_cps;
	const size_t delim_size;
};

void string_split(const char *input, Iterator &iter, ChunkCollection &result) {
	auto append_chunk = make_unique<DataChunk>();
	vector<LogicalType> types = {LogicalType::VARCHAR};
	append_chunk->Initialize(types);

	// special case: empty string
	if (iter.size == 0) {
		FlatVector::GetData<string_t>(append_chunk->data[0])[append_chunk->size()] =
		    StringVector::AddString(append_chunk->data[0], &input[0], 0);
		append_chunk->SetCardinality(append_chunk->size() + 1);
	}

	while (iter.HasNext()) {
		if (append_chunk->size() == STANDARD_VECTOR_SIZE) {
			result.count += append_chunk->size();
			result.chunks.push_back(move(append_chunk));
			append_chunk = make_unique<DataChunk>();
			append_chunk->Initialize(types);
		}

		idx_t start = iter.Start();
		idx_t end = iter.Next(input);
		size_t length = end - start;

		FlatVector::GetData<string_t>(append_chunk->data[0])[append_chunk->size()] =
		    StringVector::AddString(append_chunk->data[0], &input[start], length);
		append_chunk->SetCardinality(append_chunk->size() + 1);
	}
	if (append_chunk->size() > 0) {
		result.count += append_chunk->size();
		result.chunks.push_back(move(append_chunk));
	}
	result.Verify();
}

unique_ptr<ChunkCollection> string_split(string_t input, string_t delim) {
	const char *input_data = input.GetData();
	size_t input_size = input.GetSize();
	const char *delim_data = delim.GetData();
	size_t delim_size = delim.GetSize();

	bool ascii_only = true;
	for (auto i = 0; i < (int)input_size; ++i) {
		if (input_data[i] & 0x80) {
			ascii_only = false;
			break;
		}
	}

	auto output = make_unique<ChunkCollection>();
	vector<LogicalType> types = {LogicalType::VARCHAR};
	output->types = types;

	Iterator *iter;
	if (ascii_only) {
		iter = new AsciiIterator(input_size, delim_data, delim_size);
	} else {
		iter = new UnicodeIterator(input_size, delim_data, delim_size);
	}
	string_split(input_data, *iter, *output);

	delete iter;

	return output;
}

static void string_split_executor(DataChunk &args, ExpressionState &state, Vector &result, const bool regex) {
	VectorData input_data;
	args.data[0].Orrify(args.size(), input_data);
	auto inputs = (string_t *)input_data.data;

	VectorData delim_data;
	args.data[1].Orrify(args.size(), delim_data);
	auto delims = (string_t *)delim_data.data;

	result.Initialize(LogicalType::LIST);
	auto list_struct_data = FlatVector::GetData<list_entry_t>(result);

	auto list_child = make_unique<ChunkCollection>();
	vector<LogicalType> types = {LogicalType::VARCHAR};
	list_child->types = types;

	size_t total_len = 0;
	for (idx_t i = 0; i < args.size(); i++) {
		if ((*input_data.nullmask)[input_data.sel->get_index(i)]) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		string_t input = inputs[input_data.sel->get_index(i)];
		// TODO: what if splitter is NULL?
		string_t delim = delims[delim_data.sel->get_index(i)];

		auto split_input = string_split(input, delim);
		list_struct_data[i].length = split_input->count;
		list_struct_data[i].offset = total_len;
		total_len += split_input->count;
		list_child->Append(*split_input);
	}

	assert(list_child->count == total_len);
	result.vector_type = VectorType::CONSTANT_VECTOR;
	ListVector::SetEntry(result, move(list_child));
}

static void string_split_function(DataChunk &args, ExpressionState &state, Vector &result) {
	string_split_executor(args, state, result, false);
}

static void string_split_regex_function(DataChunk &args, ExpressionState &state, Vector &result) {
	string_split_executor(args, state, result, true);
}

void StringSplitFun::RegisterFunction(BuiltinFunctions &set) {
	child_list_t<LogicalType> child_types;
	child_types.push_back(std::make_pair("string", LogicalType::VARCHAR));
	auto varchar_list_type = LogicalType(LogicalTypeId::LIST, child_types);

	set.AddFunction({"string_split", "str_split"}, ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, varchar_list_type, string_split_function));
	// set.AddFunction(ScalarFunction({"string_split_regex", "str_split_regex"}, {LogicalType::VARCHAR, LogicalType::VARCHAR}, varchar_list_type, string_split_regex_function));
}

} // namespace duckdb
