#include "duckdb/common/complex_json.hpp"

namespace duckdb {
ComplexJSON::ComplexJSON(const string &str) : str_value(str), type(ComplexJSONType::VALUE) {
}

ComplexJSON::ComplexJSON(ComplexJSONType type_p) : type(type_p) {
}

void ComplexJSON::AddObjectEntry(const string &key, unique_ptr<ComplexJSON> object) {
	type = ComplexJSONType::OBJECT;
	obj_value[key] = std::move(object);
}

void ComplexJSON::AddArrayElement(unique_ptr<ComplexJSON> object) {
	type = ComplexJSONType::ARRAY;
	arr_value.push_back(std::move(object));
}

ComplexJSON &ComplexJSON::GetObject(const string &key) {
	if (type == ComplexJSONType::OBJECT) {
		if (obj_value.find(key) == obj_value.end()) {
			throw InvalidInputException("Complex JSON Key not found");
		}
		return *obj_value[key];
	}
	throw InvalidInputException("ComplexJson is not an object");
}

ComplexJSON &ComplexJSON::GetArrayElement(const idx_t &index) {
	if (type == ComplexJSONType::ARRAY) {
		if (index >= arr_value.size()) {
			throw InvalidInputException("Complex JSON array element out of bounds");
		}
		return *arr_value[index];
	}
	throw InvalidInputException("ComplexJson is not an array");
}

unordered_map<string, string> ComplexJSON::Flatten() const {
	unordered_map<string, string> result;
	for (auto &obj : obj_value) {
		result[obj.first] = obj.second->GetValueRecursive(*obj.second);
	}
	return result;
}

} // namespace duckdb
