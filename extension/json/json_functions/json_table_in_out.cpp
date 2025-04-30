#include "json_common.hpp"
#include "json_functions.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

enum class JSONTableInOutType { EACH, TREE };

static unique_ptr<FunctionData> JSONTableInOutBind(ClientContext &, TableFunctionBindInput &,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	const child_list_t<LogicalType> schema {
	    {"key", LogicalType::VARCHAR},     {"value", LogicalType::JSON()}, {"type", LogicalType::VARCHAR},
	    {"atom", LogicalType::JSON()},     {"id", LogicalType::UBIGINT},   {"parent", LogicalType::UBIGINT},
	    {"fullkey", LogicalType::VARCHAR}, {"path", LogicalType::VARCHAR},
	};

	// Add all default columns
	names.reserve(schema.size());
	return_types.reserve(schema.size());
	for (const auto &col : schema) {
		names.emplace_back(col.first);
		return_types.emplace_back(col.second);
	}

	return nullptr;
}

struct JSONTableInOutGlobalState : GlobalTableFunctionState {
	JSONTableInOutGlobalState() {
	}

	optional_idx json_column_index;
	optional_idx root_column_index;

	static constexpr idx_t JSON_COLUMN_OFFSET = 0;
	static constexpr idx_t ROOT_COLUMN_OFFSET = 1;
};

static unique_ptr<GlobalTableFunctionState> JSONTableInOutInitGlobal(ClientContext &, TableFunctionInitInput &input) {
	auto result = make_uniq<JSONTableInOutGlobalState>();
	for (idx_t i = 0; i < input.column_indexes.size(); i++) {
		const auto &col_idx = input.column_indexes[i];
		if (!col_idx.IsVirtualColumn()) {
			continue;
		}
		switch (col_idx.GetPrimaryIndex() - VIRTUAL_COLUMN_START) {
		case JSONTableInOutGlobalState::JSON_COLUMN_OFFSET:
			result->json_column_index = i;
			break;
		case JSONTableInOutGlobalState::ROOT_COLUMN_OFFSET:
			result->root_column_index = i;
			break;
		default:
			throw NotImplementedException("Virtual column %llu for json_each/json_tree", col_idx.GetPrimaryIndex());
		}
	}
	// TODO virtual columns require projection pushdown
	return result;
}

struct JSONTableInOutRecursionNode {
	JSONTableInOutRecursionNode(string path_p, yyjson_val *parent_val_p)
	    : path(std::move(path_p)), parent_val(parent_val_p), child_index(0) {
	}

	string path;
	yyjson_val *parent_val;
	idx_t child_index;
};

struct JSONTableInOutLocalState : LocalTableFunctionState {
	explicit JSONTableInOutLocalState(ClientContext &context)
	    : json_allocator(BufferAllocator::Get(context)), alc(json_allocator.GetYYAlc()), len(DConstants::INVALID_INDEX),
	      doc(nullptr), initialized(false) {
	}

	string GetPath() const {
		auto result = path;
		for (const auto &ri : recursion_nodes) {
			result += ri.path;
		}
		return result;
	}

	void AddRecursionIndex(yyjson_val *val, optional_ptr<yyjson_val> vkey) {
		const auto vkey_str = vkey ? string(unsafe_yyjson_get_str(vkey.get()), unsafe_yyjson_get_len(vkey.get())) : "";
		recursion_nodes.emplace_back(vkey_str, val);
	}

	JSONAllocator json_allocator;
	yyjson_alc *alc;

	string path;
	size_t len;
	yyjson_doc *doc;
	bool initialized;

	vector<JSONTableInOutRecursionNode> recursion_nodes;
};

static unique_ptr<LocalTableFunctionState> JSONTableInOutInitLocal(ExecutionContext &context, TableFunctionInitInput &,
                                                                   GlobalTableFunctionState *) {
	return make_uniq<JSONTableInOutLocalState>(context.client);
}

template <class T>
struct JSONTableInOutResultVector {
	explicit JSONTableInOutResultVector(Vector &vector_p)
	    : vector(vector_p), data(FlatVector::GetData<T>(vector)), validity(FlatVector::Validity(vector)) {
	}
	Vector &vector;
	T *data;
	ValidityMask &validity;
};

struct JSONTableInOutResult {
	explicit JSONTableInOutResult(DataChunk &output)
	    : count(0), key(output.data[0]), value(output.data[1]), type(output.data[2]), atom(output.data[3]),
	      id(output.data[4]), parent(output.data[5]), fullkey(output.data[6]), path(output.data[7]) {
	}

	template <JSONTableInOutType TYPE>
	void AddRow(JSONTableInOutLocalState &lstate, yyjson_val *val, optional_ptr<yyjson_val> vkey) {
		if (vkey) {
			key.data[count] = string_t(unsafe_yyjson_get_str(vkey.get()), unsafe_yyjson_get_len(vkey.get()));
		} else {
			key.validity.SetInvalid(count);
		}
		value.data[count] = JSONCommon::WriteVal(val, lstate.alc);
		type.data[count] = JSONCommon::ValTypeToStringT(val);
		atom.data[count] = JSONCommon::JSONValue(val, lstate.alc, atom.vector, atom.validity, count);
		id.data[count] = NumericCast<idx_t>(val - lstate.doc->root);
		if (TYPE == JSONTableInOutType::EACH || lstate.recursion_nodes.empty()) {
			parent.validity.SetInvalid(count);
		} else {
			parent.data[count] = NumericCast<uint64_t>(lstate.recursion_nodes.back().parent_val - lstate.doc->root);
		}
		const auto path_str = lstate.GetPath();
		if (vkey) {
			const auto vkey_str = string(unsafe_yyjson_get_str(vkey.get()), unsafe_yyjson_get_len(vkey.get()));
			fullkey.data[count] = StringVector::AddString(fullkey.vector, path_str + "." + vkey_str);
		} else if (!lstate.recursion_nodes.empty() && unsafe_yyjson_is_arr(lstate.recursion_nodes.back().parent_val)) {
			const auto arr_path = StringUtil::Format("[%llu]", lstate.recursion_nodes.back().child_index);
			fullkey.data[count] = StringVector::AddString(fullkey.vector, path_str + arr_path);
		} else {
			fullkey.data[count] = StringVector::AddString(fullkey.vector, path_str);
		}
		path.data[count] = StringVector::AddString(path.vector, path_str);
		count++;
	}

	idx_t count;
	JSONTableInOutResultVector<string_t> key;
	JSONTableInOutResultVector<string_t> value;
	JSONTableInOutResultVector<string_t> type;
	JSONTableInOutResultVector<string_t> atom;
	JSONTableInOutResultVector<uint64_t> id;
	JSONTableInOutResultVector<uint64_t> parent;
	JSONTableInOutResultVector<string_t> fullkey;
	JSONTableInOutResultVector<string_t> path;
};

template <JSONTableInOutType TYPE>
static bool JSONInOutTableFunctionHandleValue(JSONTableInOutLocalState &lstate, JSONTableInOutResult &result,
                                              yyjson_val *val, optional_ptr<yyjson_val> vkey) {
	if (TYPE == JSONTableInOutType::EACH) {
		result.AddRow<TYPE>(lstate, val, vkey);
		return false;
	}

	switch (yyjson_get_tag(val)) {
	default:
		result.AddRow<TYPE>(lstate, val, vkey);
		return false;
	case YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE:
	case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE:
		result.AddRow<TYPE>(lstate, val, vkey);
		lstate.AddRecursionIndex(val, vkey);
		return true;
	}
}

template <JSONTableInOutType TYPE>
static bool InitializeLocalState(JSONTableInOutLocalState &lstate, DataChunk &input, DataChunk &output,
                                 JSONTableInOutResult &result) {
	// Parse path, default to root if not given
	Value path_value("$");
	if (input.data.size() > 1) {
		path_value = ConstantVector::GetData<string_t>(input.data[1])[0];
	}

	if (JSONReadFunctionData::CheckPath(path_value, lstate.path, lstate.len) == JSONCommon::JSONPathType::WILDCARD) {
		throw BinderException("Wildcard JSON path not supported in json_each/json_tree");
	}

	if (lstate.path.c_str()[0] != '$') {
		throw BinderException("JSON path must start with '$' for json_each/json_tree");
	}

	// Parse document and get the value at the supplied path
	const auto &input_vector = input.data[0];
	if (ConstantVector::IsNull(input_vector)) {
		output.SetCardinality(0);
		return false;
	}
	const auto &input_data = FlatVector::GetData<string_t>(input_vector)[0];
	lstate.doc = JSONCommon::ReadDocument(input_data, JSONCommon::READ_FLAG, lstate.alc);
	const auto root = JSONCommon::GetUnsafe(lstate.doc->root, lstate.path.c_str(), lstate.len);

	if (!root) {
		output.SetCardinality(0);
		return false;
	}

	const auto is_arr_or_obj = unsafe_yyjson_is_arr(root) || unsafe_yyjson_is_obj(root);
	if (TYPE == JSONTableInOutType::TREE || !is_arr_or_obj) {
		result.AddRow<TYPE>(lstate, root, nullptr);
	}
	if (is_arr_or_obj) {
		lstate.AddRecursionIndex(root, nullptr);
	}

	lstate.initialized = true;
	return true;
}

template <JSONTableInOutType TYPE>
static void JSONTableInOutTraverseArray(JSONTableInOutLocalState &lstate, JSONTableInOutResult &result,
                                        yyjson_val *parent_val, idx_t &child_index) {
	size_t idx, max;
	yyjson_val *child_val;
	yyjson_arr_foreach(parent_val, idx, max, child_val) {
		if (idx < child_index) {
			continue;
		}
		if (JSONInOutTableFunctionHandleValue<TYPE>(lstate, result, child_val, nullptr)) {
			break; // We added a recursion index, break to go depth-first
		}
		if (result.count == STANDARD_VECTOR_SIZE) {
			break;
		}
		// We finished processing the array element
		child_index++;
	}
	// Array is done, get rid of it
	lstate.recursion_nodes.pop_back();
}

template <JSONTableInOutType TYPE>
static void JSONTableInOutTraverseObject(JSONTableInOutLocalState &lstate, JSONTableInOutResult &result,
                                         yyjson_val *parent_val, idx_t &child_index) {
	size_t idx, max;
	yyjson_val *child_key, *child_val;
	yyjson_obj_foreach(parent_val, idx, max, child_key, child_val) {
		if (idx < child_index) {
			continue;
		}
		if (JSONInOutTableFunctionHandleValue<TYPE>(lstate, result, child_val, child_key)) {
			break; // We added a recursion index, break to go depth-first
		}
		if (result.count == STANDARD_VECTOR_SIZE) {
			break;
		}
		// We finished processing the object field
		child_index++;
	}
	// Object is done, get rid of it
	lstate.recursion_nodes.pop_back();
}

template <JSONTableInOutType TYPE>
static OperatorResultType JSONTableInOutFunction(ExecutionContext &, TableFunctionInput &data_p, DataChunk &input,
                                                 DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<JSONTableInOutGlobalState>();
	auto &lstate = data_p.local_state->Cast<JSONTableInOutLocalState>();

	// Utility for result vectors
	JSONTableInOutResult result(output);

	// Initialize (if not yet done)
	if (!lstate.initialized) {
		if (!InitializeLocalState<TYPE>(lstate, input, output, result)) {
			return OperatorResultType::NEED_MORE_INPUT;
		}
	}

	// Set constant virtual columns ("json" and "root")
	if (gstate.json_column_index.IsValid()) {
		auto &root_vector = output.data[gstate.root_column_index.GetIndex()];
		root_vector.Reference(input.data[0]);
	}
	if (gstate.root_column_index.IsValid()) {
		auto &root_vector = output.data[gstate.json_column_index.GetIndex()];
		root_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		FlatVector::GetData<string_t>(root_vector)[0] = string_t(lstate.path.c_str(), lstate.len);
	}

	// Traverse the JSON (keeping a stack to avoid recursion and save progress across calls)
	while (!lstate.recursion_nodes.empty() && result.count != STANDARD_VECTOR_SIZE) {
		auto &parent_val = lstate.recursion_nodes.back().parent_val;
		auto &child_index = lstate.recursion_nodes.back().child_index;
		switch (yyjson_get_tag(parent_val)) {
		case YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE:
			JSONTableInOutTraverseArray<TYPE>(lstate, result, parent_val, child_index);
			break;
		case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE:
			JSONTableInOutTraverseObject<TYPE>(lstate, result, parent_val, child_index);
			break;
		default:
			throw InternalException("Non-object/array JSON added to recursion in json_each/json_tree");
		}
	}
	output.SetCardinality(result.count);

	return lstate.recursion_nodes.empty() ? OperatorResultType::NEED_MORE_INPUT : OperatorResultType::HAVE_MORE_OUTPUT;
}

virtual_column_map_t GetJSONTableInOutVirtualColumns(ClientContext &, optional_ptr<FunctionData>) {
	virtual_column_map_t result;
	result.insert(make_pair(VIRTUAL_COLUMN_START + JSONTableInOutGlobalState::JSON_COLUMN_OFFSET,
	                        TableColumn("json", LogicalType::JSON())));
	result.insert(make_pair(VIRTUAL_COLUMN_START + JSONTableInOutGlobalState::ROOT_COLUMN_OFFSET,
	                        TableColumn("root", LogicalType::VARCHAR)));
	return result;
}

template <JSONTableInOutType TYPE>
TableFunction GetJSONTableInOutFunction(const LogicalType &input_type, const bool &has_path_param) {
	vector<LogicalType> arguments = {input_type};
	if (has_path_param) {
		arguments.push_back(LogicalType::VARCHAR);
	}
	TableFunction function(arguments, nullptr, JSONTableInOutBind, JSONTableInOutInitGlobal, JSONTableInOutInitLocal);
	function.in_out_function = JSONTableInOutFunction<TYPE>;
	function.get_virtual_columns = GetJSONTableInOutVirtualColumns;
	return function;
}

TableFunctionSet JSONFunctions::GetJSONEachFunction() {
	TableFunctionSet set("json_each");
	set.AddFunction(GetJSONTableInOutFunction<JSONTableInOutType::EACH>(LogicalType::VARCHAR, false));
	set.AddFunction(GetJSONTableInOutFunction<JSONTableInOutType::EACH>(LogicalType::VARCHAR, true));
	set.AddFunction(GetJSONTableInOutFunction<JSONTableInOutType::EACH>(LogicalType::JSON(), false));
	set.AddFunction(GetJSONTableInOutFunction<JSONTableInOutType::EACH>(LogicalType::JSON(), true));
	return set;
}

TableFunctionSet JSONFunctions::GetJSONTreeFunction() {
	TableFunctionSet set("json_tree");
	set.AddFunction(GetJSONTableInOutFunction<JSONTableInOutType::TREE>(LogicalType::VARCHAR, false));
	set.AddFunction(GetJSONTableInOutFunction<JSONTableInOutType::TREE>(LogicalType::VARCHAR, true));
	set.AddFunction(GetJSONTableInOutFunction<JSONTableInOutType::TREE>(LogicalType::JSON(), false));
	set.AddFunction(GetJSONTableInOutFunction<JSONTableInOutType::TREE>(LogicalType::JSON(), true));
	return set;
}

} // namespace duckdb
