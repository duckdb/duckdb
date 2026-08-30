#include "duckdb/main/setting_preset.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/json_document.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/operator/helper/physical_reset.hpp"
#include "duckdb/execution/operator/helper/physical_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension_entries.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

namespace {

//! The available system memory. The config's filesystem is only set once the database is running,
//! so fall back to a temporary local filesystem.
idx_t AvailableMemory(DBConfig &config) {
	if (config.file_system) {
		return DBConfig::GetSystemAvailableMemory(*config.file_system);
	}
	auto local_fs = FileSystem::CreateLocal();
	return DBConfig::GetSystemAvailableMemory(*local_fs);
}

idx_t SystemThreads(DBConfig &config) {
	if (config.file_system) {
		return DBConfig::GetSystemMaxThreads(*config.file_system);
	}
	auto local_fs = FileSystem::CreateLocal();
	return DBConfig::GetSystemMaxThreads(*local_fs);
}

Value MemoryFraction(DBConfig &config, idx_t percent) {
	const auto bytes = AvailableMemory(config) / 100 * percent;
	return Value(StringUtil::Format("%lluB", static_cast<unsigned long long>(bytes)));
}

Value ThreadFraction(DBConfig &config, idx_t divisor) {
	return Value::BIGINT(NumericCast<int64_t>(MaxValue<idx_t>(SystemThreads(config) / divisor, 1)));
}

//! The default async thread count: four per core, capped
Value DefaultAsyncThreads(DBConfig &config) {
	return Value::BIGINT(NumericCast<int64_t>(MinValue<idx_t>(4 * SystemThreads(config), 256)));
}

//===--------------------------------------------------------------------===//
// host: how much of this machine may DuckDB use?
//===--------------------------------------------------------------------===//
// The three rungs differ only in degree, but the fact they encode - whether DuckDB is the main
// process on this machine - is one the engine cannot observe. Each rung sets the same settings so
// that switching between them is complete; a setting that would be identical across all three is
// left out rather than pinned to its default.

const PresetMember HOST_DEDICATED[] = {
	{"threads", [](DBConfig &c) { return ThreadFraction(c, 1); }, false},
	{"async_threads", [](DBConfig &c) { return DefaultAsyncThreads(c); }, false},
	{"max_memory", [](DBConfig &c) { return MemoryFraction(c, 80); }, false},
	{"pin_threads", [](DBConfig &) { return Value("auto"); }, false},
};

const PresetMember HOST_SHARED[] = {
	// threads before async_threads: the async pool is sized relative to it
	{"threads", [](DBConfig &c) { return ThreadFraction(c, 2); }, false},
	{"async_threads", [](DBConfig &c) { return ThreadFraction(c, 2); }, false},
	{"max_memory", [](DBConfig &c) { return MemoryFraction(c, 50); }, false},
	{"pin_threads", [](DBConfig &) { return Value("off"); }, false},
};

const PresetMember HOST_BACKGROUND[] = {
	{"threads", [](DBConfig &c) { return ThreadFraction(c, 4); }, false},
	{"async_threads", [](DBConfig &c) { return ThreadFraction(c, 4); }, false},
	{"max_memory", [](DBConfig &c) { return MemoryFraction(c, 25); }, false},
	{"pin_threads", [](DBConfig &) { return Value("off"); }, false},
};

//===--------------------------------------------------------------------===//
// network: how reliable is the link to remote storage?
//===--------------------------------------------------------------------===//
// autoload_known_extensions comes first so that the httpfs settings below can be resolved; they
// are optional because httpfs may not be installed.

const PresetMember NETWORK_FLAKY[] = {
	{"autoload_known_extensions", [](DBConfig &) { return Value::BOOLEAN(true); }, false},
	{"async_threads", [](DBConfig &c) { return ThreadFraction(c, 1); }, false},
	{"http_retries", [](DBConfig &) { return Value::BIGINT(10); }, true},
	{"http_retry_backoff", [](DBConfig &) { return Value::FLOAT(2); }, true},
	{"http_timeout", [](DBConfig &) { return Value::BIGINT(120000); }, true},
};

//===--------------------------------------------------------------------===//
// memory / execution
//===--------------------------------------------------------------------===//

const PresetMember MEMORY_NO_SPILL[] = {
	{"max_temp_directory_size", [](DBConfig &) { return Value("0B"); }, false},
};

const PresetMember EXECUTION_BULK_INGEST[] = {
	{"preserve_insertion_order", [](DBConfig &) { return Value::BOOLEAN(false); }, false},
	{"checkpoint_threshold", [](DBConfig &) { return Value("1GB"); }, false},
};

#define DUCKDB_PRESET(NAME, DESC, MEMBERS) {NAME, DESC, MEMBERS, sizeof(MEMBERS) / sizeof(PresetMember)}

const PresetDefinition BUILTIN_PRESETS[] = {
	DUCKDB_PRESET("execution:bulk_ingest", "Bulk loading rather than querying", EXECUTION_BULK_INGEST),
	DUCKDB_PRESET("host:background", "DuckDB is not the main process on this machine", HOST_BACKGROUND),
	DUCKDB_PRESET("host:dedicated", "DuckDB may use the whole machine", HOST_DEDICATED),
	DUCKDB_PRESET("host:shared", "DuckDB shares this machine with other work", HOST_SHARED),
	DUCKDB_PRESET("memory:no_spill", "Fail rather than spill to disk", MEMORY_NO_SPILL),
	DUCKDB_PRESET("network:flaky", "The link to remote storage drops or rate-limits", NETWORK_FLAKY),
};

} // namespace

idx_t PresetRegistry::GetBuiltinCount() {
	return sizeof(BUILTIN_PRESETS) / sizeof(PresetDefinition);
}

const PresetDefinition &PresetRegistry::GetBuiltinByIndex(idx_t index) {
	D_ASSERT(index < GetBuiltinCount());
	return BUILTIN_PRESETS[index];
}

optional_ptr<const PresetDefinition> PresetRegistry::FindBuiltin(const string &name) {
	for (idx_t i = 0; i < GetBuiltinCount(); i++) {
		if (StringUtil::CIEquals(BUILTIN_PRESETS[i].name, name)) {
			return &BUILTIN_PRESETS[i];
		}
	}
	return nullptr;
}

Preset PresetRegistry::Materialize(const PresetDefinition &definition, DBConfig &config) {
	Preset result;
	result.name = definition.name;
	result.description = definition.description;
	result.source = "builtin";
	for (idx_t i = 0; i < definition.member_count; i++) {
		auto &member = definition.members[i];
		PresetSetting setting;
		setting.name = member.setting;
		setting.value = member.value(config);
		setting.optional = member.optional;
		result.settings.push_back(std::move(setting));
	}
	return result;
}

namespace {

//! Convert one JSON value from a preset file into the value to assign.
Value PresetValueFromJSON(const string &setting, const JSONValue &value) {
	switch (value.GetType()) {
	case JSONValueType::BOOLEAN:
		return Value::BOOLEAN(value.GetBoolean());
	case JSONValueType::SIGNED_INTEGER:
		return Value::BIGINT(value.GetSignedInteger());
	case JSONValueType::UNSIGNED_INTEGER:
		return Value::UBIGINT(value.GetUnsignedInteger());
	case JSONValueType::DOUBLE:
		return Value::DOUBLE(value.GetDouble());
	case JSONValueType::STRING:
		return Value(value.GetString());
	case JSONValueType::ARRAY: {
		vector<Value> children;
		value.IterateArray([&](JSONValue element) { children.emplace_back(element.ToString()); });
		for (auto &child : children) {
			// strings arrive quoted from ToString, so re-read them as plain strings
			auto text = StringValue::Get(child);
			if (text.size() >= 2 && text.front() == '"' && text.back() == '"') {
				child = Value(text.substr(1, text.size() - 2));
			}
		}
		return Value::LIST(LogicalType::VARCHAR, std::move(children));
	}
	default:
		throw InvalidInputException("Preset setting \"%s\" has a value that is not a string, number, boolean, "
		                            "list or null",
		                            setting);
	}
}

} // namespace

Preset PresetRegistry::LoadFromFile(ClientContext &context, const string &path) {
	auto &fs = FileSystem::GetFileSystem(context);
	if (!fs.FileExists(path)) {
		throw IOException("Preset file \"%s\" does not exist", path);
	}
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	auto file_size = NumericCast<idx_t>(fs.GetFileSize(*handle));
	string contents(file_size, '\0');
	fs.Read(*handle, contents.data(), NumericCast<int64_t>(file_size));

	JSONParseError error;
	auto doc = JSONDocument::TryParse(contents.c_str(), contents.size(), error, JSONReadFlags::NONE);
	if (!doc) {
		throw InvalidInputException("Preset file \"%s\" is not valid JSON", path);
	}
	auto root = doc->GetRoot();
	if (!root.IsObject()) {
		throw InvalidInputException("Preset file \"%s\" must contain a JSON object", path);
	}

	Preset result;
	result.name = path;
	result.source = "file";
	auto description = root.GetMember("description");
	if (description.IsValid() && description.IsString()) {
		result.description = description.GetString();
	}
	auto settings = root.GetMember("settings");
	if (!settings.IsValid() || !settings.IsArray()) {
		// an array rather than an object, because JSON objects are unordered and the order settings
		// are applied in is significant
		throw InvalidInputException("Preset file \"%s\" must contain a \"settings\" array", path);
	}
	settings.IterateArray([&](JSONValue element) {
		if (!element.IsObject()) {
			throw InvalidInputException("Each entry of \"settings\" must be an object of one setting, in \"%s\"",
			                            path);
		}
		idx_t members = 0;
		element.IterateObject([&](const string &key, JSONValue value) {
			members++;
			PresetSetting setting;
			setting.name = key;
			if (value.IsNull()) {
				setting.is_reset = true;
			} else {
				setting.value = PresetValueFromJSON(key, value);
			}
			result.settings.push_back(std::move(setting));
		});
		if (members != 1) {
			throw InvalidInputException("Each entry of \"settings\" must hold exactly one setting, in \"%s\"", path);
		}
	});
	// check every setting up front: applying a preset is not atomic, so a name that does not exist
	// must be caught before any of the preceding settings have been applied
	auto &config = DBConfig::GetConfig(context);
	for (auto &setting : result.settings) {
		Identifier name(setting.name);
		if (DBConfig::GetOptionByName(name)) {
			continue;
		}
		ExtensionOption extension_option;
		if (config.TryGetExtensionOption(name, extension_option)) {
			continue;
		}
		// a setting owned by an extension that is not loaded yet is fine - it autoloads on use
		if (!ExtensionHelper::FindExtensionInEntries(name, EXTENSION_SETTINGS).empty()) {
			continue;
		}
		throw CatalogException("Preset file \"%s\" sets unrecognized configuration parameter \"%s\"", path,
		                       setting.name);
	}
	return result;
}

//===--------------------------------------------------------------------===//
// PresetManager
//===--------------------------------------------------------------------===//

void PresetManager::Register(Preset preset) {
	lock_guard<mutex> guard(lock);
	auto name = preset.name;
	presets[name] = std::move(preset);
}

bool PresetManager::TryGet(const string &name, Preset &result) const {
	lock_guard<mutex> guard(lock);
	auto entry = presets.find(name);
	if (entry == presets.end()) {
		return false;
	}
	result = entry->second;
	return true;
}

vector<Preset> PresetManager::All() const {
	lock_guard<mutex> guard(lock);
	vector<Preset> result;
	for (auto &entry : presets) {
		result.push_back(entry.second);
	}
	return result;
}

void PresetManager::SetDirectory(string path) {
	lock_guard<mutex> guard(lock);
	directory = std::move(path);
}

void PresetManager::ResetDirectory() {
	lock_guard<mutex> guard(lock);
	directory.clear();
}

string PresetManager::GetDirectory() const {
	lock_guard<mutex> guard(lock);
	if (!directory.empty()) {
		return directory;
	}
	// a local filesystem is all that is needed to resolve the home directory, as for secrets
	auto fs = FileSystem::CreateLocal();
	return fs->JoinPath(fs->JoinPath(fs->GetHomeDirectory(), ".duckdb"), "presets");
}

//===--------------------------------------------------------------------===//
// Persistence
//===--------------------------------------------------------------------===//

string PresetRegistry::GetPresetPath(ClientContext &context, const string &name) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto &config = DBConfig::GetConfig(context);
	auto path = config.preset_manager->GetDirectory();
	// a namespaced name becomes a subdirectory: "host:shared" -> host/shared.json. A colon is not a
	// legal filename character on every platform, so it cannot simply be kept.
	auto parts = StringUtil::Split(name, ':');
	for (idx_t i = 0; i + 1 < parts.size(); i++) {
		path = fs.JoinPath(path, parts[i]);
	}
	return fs.JoinPath(path, parts.back() + ".json");
}

string PresetRegistry::ToJSON(const Preset &preset) {
	JSONWriter writer;
	auto root = writer.CreateObject();
	if (!preset.description.empty()) {
		root.AddString("description", preset.description);
	}
	auto settings = writer.CreateArray();
	for (auto &setting : preset.settings) {
		auto entry = writer.CreateObject();
		if (setting.is_reset || setting.value.IsNull()) {
			entry.Add(setting.name, writer.CreateNull());
		} else if (setting.value.type().id() == LogicalTypeId::BOOLEAN) {
			entry.Add(setting.name, writer.CreateBoolean(BooleanValue::Get(setting.value)));
		} else if (setting.value.type().IsIntegral()) {
			entry.Add(setting.name, writer.CreateSignedInteger(setting.value.GetValue<int64_t>()));
		} else if (setting.value.type().id() == LogicalTypeId::LIST) {
			auto list = writer.CreateArray();
			for (auto &child : ListValue::GetChildren(setting.value)) {
				list.AppendString(child.ToString());
			}
			entry.Add(setting.name, std::move(list));
		} else {
			entry.AddString(setting.name, setting.value.ToString());
		}
		settings.Append(std::move(entry));
	}
	root.Add("settings", std::move(settings));
	writer.SetRoot(std::move(root));
	return writer.ToString(JSONWriteFlags::PRETTY);
}

void PresetRegistry::Persist(ClientContext &context, const Preset &preset) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto path = GetPresetPath(context, preset.name);
	auto directory = fs.JoinPath(path, "..");
	fs.CreateDirectoriesRecursive(StringUtil::GetFilePath(path));

	auto contents = ToJSON(preset);
	// written 0600, like a persistent secret: a preset is executed, so it must not be writable by
	// anyone but its owner
	auto flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE | FileFlags::FILE_FLAGS_PRIVATE;
	auto handle = fs.OpenFile(path, flags);
	handle->Write(QueryContext(context), contents.data(), contents.size(), 0);
	handle->Truncate(NumericCast<int64_t>(contents.size()));
}

Preset PresetRegistry::Resolve(ClientContext &context, const string &name) {
	auto &config = DBConfig::GetConfig(context);
	// most local wins, so that a preset defined here overrides one shipped with DuckDB
	Preset registered;
	if (config.preset_manager->TryGet(name, registered)) {
		return registered;
	}
	// the name maps to a path, so a miss costs one failed open rather than a directory scan
	auto path = GetPresetPath(context, name);
	auto &fs = FileSystem::GetFileSystem(context);
	if (fs.FileExists(path)) {
		auto result = LoadFromFile(context, path);
		result.name = name;
		return result;
	}
	auto builtin = FindBuiltin(name);
	if (builtin) {
		return Materialize(*builtin, config);
	}
	vector<string> candidates;
	for (idx_t i = 0; i < GetBuiltinCount(); i++) {
		candidates.emplace_back(GetBuiltinByIndex(i).name);
	}
	for (auto &preset : config.preset_manager->All()) {
		candidates.push_back(preset.name);
	}
	throw CatalogException("Preset \"%s\" does not exist (looked for %s)\nAvailable presets: %s", name, path,
	                       StringUtil::Join(candidates, ", "));
}

vector<PresetApplyResult> PresetRegistry::Apply(ClientContext &context, const Preset &preset) {
	vector<PresetApplyResult> results;
	for (auto &setting : preset.settings) {
		Identifier name(setting.name);

		PresetApplyResult result;
		result.setting = setting.name;
		Value previous;
		if (!context.TryGetCurrentSetting(name, previous) && setting.optional) {
			result.status = "skipped";
			result.note = "setting is not available";
			results.push_back(std::move(result));
			continue;
		}
		result.old_value = previous;
		if (setting.is_reset) {
			PhysicalReset::ResetVariable(context, name, SetScope::AUTOMATIC);
			result.status = "reset";
		} else {
			PhysicalSet::SetVariable(context, name, SetScope::AUTOMATIC, setting.value);
			result.status = "applied";
		}
		Value applied;
		context.TryGetCurrentSetting(name, applied);
		result.new_value = applied;
		results.push_back(std::move(result));
	}
	return results;
}

} // namespace duckdb
