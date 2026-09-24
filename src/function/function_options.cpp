#include "duckdb/function/function_options.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

FunctionOptionDefinition::FunctionOptionDefinition(Identifier name_p, LogicalType type_p)
    : name(std::move(name_p)), type(std::move(type_p)) {
}

bool FunctionOptionDefinition::operator==(const FunctionOptionDefinition &other) const {
	return name == other.name && aliases == other.aliases && type == other.type;
}

bool FunctionOptionDefinition::operator!=(const FunctionOptionDefinition &other) const {
	return !(*this == other);
}

string FunctionOptionDefinition::ToString() const {
	return StringUtil::Format("%s %s", SQLIdentifier(name), type.ToString());
}

FunctionOptionSchema &FunctionOptionSchema::Add(Identifier name, LogicalType type) {
	options.emplace_back(std::move(name), std::move(type));
	return *this;
}

FunctionOptionSchema &FunctionOptionSchema::Alias(Identifier alias) {
	if (options.empty()) {
		throw InternalException("FunctionOptionSchema::Alias called before any option was added");
	}
	options.back().aliases.push_back(std::move(alias));
	return *this;
}

FunctionOptionSchema FunctionOptionSchema::Merge(const FunctionOptionSchema &other) const {
	auto result = *this;
	result.options.insert(result.options.end(), other.options.begin(), other.options.end());
	return result;
}

optional_ptr<const FunctionOptionDefinition> FunctionOptionSchema::Find(const Identifier &name) const {
	// names are matched case-insensitively, like parameter names
	for (auto &option : options) {
		if (option.name == name) {
			return option;
		}
		for (auto &alias : option.aliases) {
			if (alias == name) {
				return option;
			}
		}
	}
	return nullptr;
}

const vector<FunctionOptionDefinition> &FunctionOptionSchema::GetOptions() const {
	return options;
}

void FunctionOptionSchema::Verify() const {
	identifier_set_t names;
	for (auto &option : options) {
		if (!names.insert(option.name).second) {
			throw InvalidInputException("Duplicate option name: %s", option.name);
		}
		for (auto &alias : option.aliases) {
			if (!names.insert(alias).second) {
				throw InvalidInputException("Duplicate option name: %s", alias);
			}
		}
	}
}

vector<Identifier> FunctionOptionSchema::GetNames() const {
	vector<Identifier> result;
	for (auto &option : options) {
		result.push_back(option.name);
		for (auto &alias : option.aliases) {
			result.push_back(alias);
		}
	}
	return result;
}

bool FunctionOptionSchema::operator==(const FunctionOptionSchema &other) const {
	return options == other.options;
}

bool FunctionOptionSchema::operator!=(const FunctionOptionSchema &other) const {
	return !(*this == other);
}

hash_t FunctionOptionSchema::Hash() const {
	hash_t hash = duckdb::Hash(options.size());
	for (auto &option : options) {
		hash = CombineHash(hash, IdentifierHashFunction()(option.name));
		hash = CombineHash(hash, option.type.Hash());
	}
	return hash;
}

} // namespace duckdb
