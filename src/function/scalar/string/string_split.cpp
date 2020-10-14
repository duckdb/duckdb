#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_size.hpp"
#include "utf8proc_wrapper.hpp"
#include "utf8proc.hpp"

#include "duckdb/function/scalar/regexp.hpp"

using namespace std;

namespace duckdb {

struct Iterator {
public:
	Iterator(const size_t size) : size(size) {
	}
	virtual ~Iterator() {
	}
	virtual idx_t Next(const char *input) {
		return 0;
	}
	bool HasNext() {
		return offset < size;
	}
	idx_t Start() {
		return start;
	}
	const size_t size;

protected:
	idx_t start = 0;  // end of last place a delim match was found
	idx_t offset = 0; // current position
};

struct AsciiIterator : virtual public Iterator {
public:
	AsciiIterator(size_t size, const char *delim, const size_t delim_size)
	    : Iterator(size), delim(delim), delim_size(delim_size) {
	}
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
				idx_t i;
				for (i = 1; i < delim_size; i++) {
					if (input[offset + i] != delim[i])
						break;
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
	UnicodeIterator(size_t input_size, const char *delim, const size_t delim_size)
	    : Iterator(input_size), delim_size(delim_size) {
		int cp_sz;
		for (idx_t i = 0; i < delim_size; i += cp_sz) {
			delim_cps.push_back(utf8proc_codepoint(delim, cp_sz));
		}
	}
	idx_t Next(const char *input) override {
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
				for (idx_t i = 1; i < delim_cps.size(); i++) {
					if (utf8proc_codepoint(&input[offset + delim_offset], cp_sz) != delim_cps[i])
						break;
					delim_offset += cp_sz;
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

struct RegexIterator : virtual public Iterator {
public:
	RegexIterator(size_t input_size, unique_ptr<RE2> re, const bool ascii_only)
	    : Iterator(input_size), re(move(re)), ascii_only(ascii_only) {
	}
	idx_t Next(const char *input) override {
		duckdb_re2::StringPiece input_sp(input, size);
		duckdb_re2::StringPiece match;
		if (re->Match(input_sp, start, size, RE2::UNANCHORED, &match, 1)) {
			offset = match.data() - input;
			// special case: 0 length match
			if (match.size() == 0 && start < size) {
				if (ascii_only)
					offset++;
				else
					offset = utf8proc_next_grapheme(input, size, offset);
				start = offset;
			} else {
				start = offset + match.size();
			}
		} else {
			offset = size;
		}
		return offset;
	}

protected:
	unique_ptr<RE2> re;
	const bool ascii_only;
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

		result.count += append_chunk->size();
		result.chunks.push_back(move(append_chunk));

		result.Verify();
		return;
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

unique_ptr<ChunkCollection> string_split(string_t input, string_t delim, const bool regex) {
	const char *input_data = input.GetData();
	size_t input_size = input.GetSize();
	const char *delim_data = delim.GetData();
	size_t delim_size = delim.GetSize();

	bool ascii_only = Utf8Proc::Analyze(input_data, input_size) == UnicodeType::ASCII;

	auto output = make_unique<ChunkCollection>();
	vector<LogicalType> types = {LogicalType::VARCHAR};
	output->types = types;

	unique_ptr<Iterator> iter;
	if (regex) {
		auto re = make_unique<RE2>(duckdb_re2::StringPiece(delim_data, delim_size));
		if (!re->ok()) {
			throw Exception(re->error());
		}
		iter = make_unique_base<Iterator, RegexIterator>(input_size, move(re), ascii_only);
	} else if (ascii_only) {
		iter = make_unique_base<Iterator, AsciiIterator>(input_size, delim_data, delim_size);
	} else {
		iter = make_unique_base<Iterator, UnicodeIterator>(input_size, delim_data, delim_size);
	}
	string_split(input_data, *iter, *output);

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

		unique_ptr<ChunkCollection> split_input;
		if ((*delim_data.nullmask)[delim_data.sel->get_index(i)]) {
			// special case: delimiter is NULL
			split_input = make_unique<ChunkCollection>();
			split_input->types = types;

			auto append_chunk = make_unique<DataChunk>();
			append_chunk->Initialize(types);
			FlatVector::GetData<string_t>(append_chunk->data[0])[append_chunk->size()] =
			    StringVector::AddString(append_chunk->data[0], input);
			append_chunk->SetCardinality(append_chunk->size() + 1);

			split_input->count += append_chunk->size();
			split_input->chunks.push_back(move(append_chunk));
		} else {
			string_t delim = delims[delim_data.sel->get_index(i)];
			split_input = string_split(input, delim, regex);
		}
		list_struct_data[i].length = split_input->count;
		list_struct_data[i].offset = total_len;
		total_len += split_input->count;
		list_child->Append(*split_input);
	}

	assert(list_child->count == total_len);
	if (args.data[0].vector_type == VectorType::CONSTANT_VECTOR &&
	    args.data[1].vector_type == VectorType::CONSTANT_VECTOR)
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

	set.AddFunction({"string_split", "str_split"}, ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                                              varchar_list_type, string_split_function));
	set.AddFunction(
	    {"string_split_regex", "str_split_regex"},
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, varchar_list_type, string_split_regex_function));
}

} // namespace duckdb
