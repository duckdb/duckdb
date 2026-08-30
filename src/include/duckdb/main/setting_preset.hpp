//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/setting_preset.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {
class ClientContext;
class DBConfig;

//! One setting within a preset, resolved to a concrete value.
struct PresetSetting {
	string name;
	//! The value to apply, unless is_reset is set
	Value value;
	//! Reset the setting to its default rather than assigning a value. A preset expresses this as a
	//! null: assigning null would pin the setting at null in whichever layer it was set, which is
	//! never what is meant.
	bool is_reset = false;
	//! Whether the preset still applies when this setting does not exist, for settings owned by an
	//! extension that may not be installed
	bool optional = false;
};

//! A preset resolved for the machine it is about to be applied on.
struct Preset {
	string name;
	string description;
	//! Where the preset came from: "builtin" or "file"
	string source;
	//! Applied in order. Settings constrain each other, so for example threads is set before
	//! async_threads is derived from it, and autoloading is enabled before any setting owned by an
	//! extension is touched.
	vector<PresetSetting> settings;
};

//! One member of a built-in preset. Values are produced rather than stored so that a preset can
//! size itself to the machine it is applied on instead of baking in absolute numbers.
struct PresetMember {
	const char *setting;
	Value (*value)(DBConfig &config);
	bool optional;
};

//! A built-in preset, defined in code.
struct PresetDefinition {
	const char *name;
	const char *description;
	const PresetMember *members;
	idx_t member_count;
};

//! What applying one setting of a preset did.
struct PresetApplyResult {
	string setting;
	Value old_value;
	Value new_value;
	//! "applied", "reset", or "skipped"
	string status;
	//! Why the setting was skipped; empty otherwise
	string note;
};

struct PresetRegistry {
	//! The presets defined in code
	DUCKDB_API static idx_t GetBuiltinCount();
	DUCKDB_API static const PresetDefinition &GetBuiltinByIndex(idx_t index);
	DUCKDB_API static optional_ptr<const PresetDefinition> FindBuiltin(const string &name);

	//! Resolve a built-in preset's values for this machine
	DUCKDB_API static Preset Materialize(const PresetDefinition &definition, DBConfig &config);
	//! Read a preset from a JSON file
	DUCKDB_API static Preset LoadFromFile(ClientContext &context, const string &path);
	//! Look up a preset by name
	DUCKDB_API static Preset Resolve(ClientContext &context, const string &name);

	//! Apply a preset. Settings are applied in order through the regular SET and RESET paths, so
	//! configuration locking, extension autoloading and scope resolution behave as they do for an
	//! explicit statement.
	DUCKDB_API static vector<PresetApplyResult> Apply(ClientContext &context, const Preset &preset);
};

} // namespace duckdb
