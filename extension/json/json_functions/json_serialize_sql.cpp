#include "json_executors.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/common/serializer/format_serializer.hpp"

namespace duckdb {

struct JsonSerializer : FormatSerializer {
private:
	yyjson_mut_doc *doc;
	yyjson_mut_val *current_tag;
	vector<yyjson_mut_val *> stack;
	bool skip_if_null = false;

	inline yyjson_mut_val *Current() {
		return stack.back();
	};

	// Either adds a value to the current object with the current tag, or appends it to the current array
	void push_value(yyjson_mut_val *val) {

		auto current = Current();
		// Array case, just append the value
		if (yyjson_mut_is_arr(current)) {
			yyjson_mut_arr_append(current, val);
		}
		// Object case, use the currently set tag.
		else if (yyjson_mut_is_obj(current)) {
			yyjson_mut_obj_add(current, current_tag, val);
		}
		// Else throw
		else {
			throw InternalException("Cannot add value to non-array/object json value");
		}
	}

public:
	explicit JsonSerializer(yyjson_mut_doc *doc, bool skip_if_null) : doc(doc), stack({yyjson_mut_obj(doc)}), skip_if_null(skip_if_null) {
		serialize_enum_as_string = true;
	}

	yyjson_mut_val *GetRootObject() {
		D_ASSERT(stack.size() == 1); // or we forgot to pop somewhere
		return stack.front();
	};

	void BeginWriteOptional(bool present) override {
		// Always write the tag for optional values, just set the value to null if not present.
		if(!present) {
			WriteNull();
		}
		// TODO: allow skipping writing null properties
	}

	void BeginWriteList(idx_t count) override {
		auto new_value = yyjson_mut_arr(doc);
		push_value(new_value);
		stack.push_back(new_value);
	}

	void EndWriteList(idx_t count) override {
		stack.pop_back();
	}

	void BeginWriteMap(idx_t count) override {
		auto new_value = yyjson_mut_obj(doc);
		push_value(new_value);
		stack.push_back(new_value);
	}

	void EndWriteMap(idx_t count) override {
		stack.pop_back();
	}

	void BeginWriteObject() override {
		auto new_value = yyjson_mut_obj(doc);
		push_value(new_value);
		stack.push_back(new_value);
	}

	void EndWriteObject() override {
		stack.pop_back();
	}

	void WriteTag(const char *tag) override {
		current_tag = yyjson_mut_strcpy(doc, tag);
	}

	void WriteNull() override {
	    auto val = yyjson_mut_null(doc);
		push_value(val);
	}

	void WriteValue(uint8_t value) override {
		auto val = yyjson_mut_uint(doc, value);
		push_value(val);
	}

	void WriteValue(int8_t value) override {
		auto val = yyjson_mut_sint(doc, value);
		push_value(val);
	}

	void WriteValue(uint16_t value) override {
		auto val = yyjson_mut_uint(doc, value);
		push_value(val);
	}

	void WriteValue(int16_t value) override {
		auto val = yyjson_mut_sint(doc, value);
		push_value(val);
	}

	void WriteValue(uint32_t value) override {
		auto val = yyjson_mut_uint(doc, value);
		push_value(val);
	}

	void WriteValue(int32_t value) override {
		auto val = yyjson_mut_int(doc, value);
		push_value(val);
	}

	void WriteValue(uint64_t value) override {
		auto val = yyjson_mut_uint(doc, value);
		push_value(val);
	}

	void WriteValue(int64_t value) override {
		auto val = yyjson_mut_sint(doc, value);
		push_value(val);
	}

	void WriteValue(hugeint_t value) override {
		throw NotImplementedException("Cannot serialize hugeint to json yet!");
	}

	void WriteValue(float value) override {
		auto val = yyjson_mut_real(doc, value);
		push_value(val);
	}

	void WriteValue(double value) override {
		auto val = yyjson_mut_real(doc, value);
		push_value(val);
	}

	void WriteValue(interval_t value) override {
		throw NotImplementedException("Cannot serialize interval_t to json yet!");
	}

	void WriteValue(const string &value) override {
		auto val = yyjson_mut_strcpy(doc, value.c_str());
		push_value(val);
	}

	void WriteValue(const string_t value) override {
		auto str = value.GetString();
		auto val = yyjson_mut_strcpy(doc, str.c_str());
		push_value(val);
	}

	void WriteValue(const char *value) override {
		auto val = yyjson_mut_strcpy(doc, value);
		push_value(val);
	}

	void WriteValue(bool value) override {
		auto val = yyjson_mut_bool(doc, value);
		push_value(val);
	}
};

static yyjson_mut_val *Serialize(SelectStatement &stmt, yyjson_mut_doc *doc) {
	auto serialize = JsonSerializer(doc, false);
	stmt.FormatSerialize(serialize);
	return serialize.GetRootObject();
}

static void JsonSerializeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator.GetYYJSONAllocator();
	auto &inputs = args.data[0];

	UnaryExecutor::Execute<string_t, string_t>(inputs, result, args.size(), [&](string_t input) {
		auto parser = Parser();
		parser.ParseQuery(input.GetString());

		auto doc = JSONCommon::CreateDocument(alc);
		auto arr = yyjson_mut_arr(doc);
		yyjson_mut_doc_set_root(doc, arr);

		for (auto &statement : parser.statements) {
			if (statement->type != StatementType::SELECT_STATEMENT) {
				throw NotImplementedException("Only SELECT statements can be serialized to json!");
			}
			auto &select = (SelectStatement &)*statement;
			auto json = Serialize(select, doc);
			yyjson_mut_arr_append(arr, json);
		}

		return JSONCommon::WriteVal(arr, alc);
	});
}

CreateScalarFunctionInfo JSONFunctions::GetSerializeSqlFunction() {
	auto func = ScalarFunction("json_serialize_sql", {LogicalType::VARCHAR}, JSONCommon::JSONType(),
	                           JsonSerializeFunction, nullptr, nullptr, nullptr, JSONFunctionLocalState::Init);
	return CreateScalarFunctionInfo(func);
}

} // namespace duckdb
