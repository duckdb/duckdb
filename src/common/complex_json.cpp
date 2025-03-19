#include "duckdb/common/complex_json.hpp"

namespace duckdb {
ComplexJSON::ComplexJSON(const string &str) : str_value(str), is_object(false) {
}

ComplexJSON::ComplexJSON() : is_object(false) {
}

void ComplexJSON::AddObject(const string &key, unique_ptr<ComplexJSON> object) {
	is_object = true;
	obj_value[key] = std::move(object);
}

ComplexJSON &ComplexJSON::GetObject(const string &key) {
	if (is_object) {
		if (obj_value.find(key) == obj_value.end()) {
			throw InvalidInputException("Complex JSON Key not found");
		}
		return *obj_value[key];
	}
	throw InvalidInputException("ComplexJson is not an object");
}

unordered_map<string, string> ComplexJSON::Flatten() const {
	unordered_map<string, string> result;
	for (auto &obj : obj_value) {
		result[obj.first] = obj.second->GetValueRecursive(*obj.second);
	}
	return result;
}

} // namespace duckdb
