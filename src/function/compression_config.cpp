#include "duckdb/common/pair.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

typedef CompressionFunction (*get_compression_function_t)(PhysicalType type);
typedef bool (*compression_supports_type_t)(const PhysicalType physical_type);

struct DefaultCompressionMethod {
	CompressionType type;
	get_compression_function_t get_function;
	compression_supports_type_t supports_type;
};

static const DefaultCompressionMethod internal_compression_methods[] = {
    {CompressionType::COMPRESSION_CONSTANT, ConstantFun::GetFunction, ConstantFun::TypeIsSupported},
    {CompressionType::COMPRESSION_UNCOMPRESSED, UncompressedFun::GetFunction, UncompressedFun::TypeIsSupported},
    {CompressionType::COMPRESSION_RLE, RLEFun::GetFunction, RLEFun::TypeIsSupported},
    {CompressionType::COMPRESSION_BITPACKING, BitpackingFun::GetFunction, BitpackingFun::TypeIsSupported},
    {CompressionType::COMPRESSION_DICTIONARY, DictionaryCompressionFun::GetFunction,
     DictionaryCompressionFun::TypeIsSupported},
    {CompressionType::COMPRESSION_CHIMP, ChimpCompressionFun::GetFunction, ChimpCompressionFun::TypeIsSupported},
    {CompressionType::COMPRESSION_PATAS, PatasCompressionFun::GetFunction, PatasCompressionFun::TypeIsSupported},
    {CompressionType::COMPRESSION_ALP, AlpCompressionFun::GetFunction, AlpCompressionFun::TypeIsSupported},
    {CompressionType::COMPRESSION_ALPRD, AlpRDCompressionFun::GetFunction, AlpRDCompressionFun::TypeIsSupported},
    {CompressionType::COMPRESSION_FSST, FSSTFun::GetFunction, FSSTFun::TypeIsSupported},
    {CompressionType::COMPRESSION_ZSTD, ZSTDFun::GetFunction, ZSTDFun::TypeIsSupported},
    {CompressionType::COMPRESSION_ROARING, RoaringCompressionFun::GetFunction, RoaringCompressionFun::TypeIsSupported},
    {CompressionType::COMPRESSION_EMPTY, EmptyValidityCompressionFun::GetFunction,
     EmptyValidityCompressionFun::TypeIsSupported},
    {CompressionType::COMPRESSION_DICT_FSST, DictFSSTCompressionFun::GetFunction,
     DictFSSTCompressionFun::TypeIsSupported},
    {CompressionType::COMPRESSION_AUTO, nullptr, nullptr}};

idx_t CompressionFunctionSet::GetCompressionIndex(PhysicalType physical_type) {
	switch (physical_type) {
	case PhysicalType::BOOL:
		return 0;
	case PhysicalType::UINT8:
		return 1;
	case PhysicalType::INT8:
		return 2;
	case PhysicalType::UINT16:
		return 3;
	case PhysicalType::INT16:
		return 4;
	case PhysicalType::UINT32:
		return 5;
	case PhysicalType::INT32:
		return 6;
	case PhysicalType::UINT64:
		return 7;
	case PhysicalType::INT64:
		return 8;
	case PhysicalType::FLOAT:
		return 9;
	case PhysicalType::DOUBLE:
		return 10;
	case PhysicalType::INTERVAL:
		return 11;
	case PhysicalType::LIST:
		return 12;
	case PhysicalType::STRUCT:
		return 13;
	case PhysicalType::ARRAY:
		return 14;
	case PhysicalType::VARCHAR:
		return 15;
	case PhysicalType::UINT128:
		return 16;
	case PhysicalType::INT128:
		return 17;
	case PhysicalType::BIT:
		return 18;
	default:
		throw InternalException("Unsupported physical type for compression index");
	}
}

idx_t CompressionFunctionSet::GetCompressionIndex(CompressionType type) {
	return static_cast<idx_t>(type);
}

CompressionFunctionSet::CompressionFunctionSet() {
	for (idx_t i = 0; i < PHYSICAL_TYPE_COUNT; i++) {
		is_loaded[i] = false;
	}
	ResetDisabledMethods();
	functions.resize(PHYSICAL_TYPE_COUNT);
}

bool EmitCompressionFunction(CompressionType type) {
	switch (type) {
	case CompressionType::COMPRESSION_UNCOMPRESSED:
	case CompressionType::COMPRESSION_RLE:
	case CompressionType::COMPRESSION_BITPACKING:
	case CompressionType::COMPRESSION_DICTIONARY:
	case CompressionType::COMPRESSION_CHIMP:
	case CompressionType::COMPRESSION_PATAS:
	case CompressionType::COMPRESSION_ALP:
	case CompressionType::COMPRESSION_ALPRD:
	case CompressionType::COMPRESSION_FSST:
	case CompressionType::COMPRESSION_ZSTD:
	case CompressionType::COMPRESSION_ROARING:
	case CompressionType::COMPRESSION_DICT_FSST:
		return true;
	default:
		return false;
	}
}

vector<reference<const CompressionFunction>>
CompressionFunctionSet::GetCompressionFunctions(PhysicalType physical_type) {
	LoadCompressionFunctions(physical_type);
	auto index = GetCompressionIndex(physical_type);
	auto &function_list = functions[index];
	vector<reference<const CompressionFunction>> result;
	for (auto &entry : function_list) {
		auto compression_index = GetCompressionIndex(entry.type);
		if (is_disabled[compression_index]) {
			// explicitly disabled
			continue;
		}
		if (!EmitCompressionFunction(entry.type)) {
			continue;
		}
		result.push_back(entry);
	}
	return result;
}

CompressionFunctionSetLoadResult CompressionFunctionSet::LoadCompressionFunctions(PhysicalType physical_type) {
	auto index = GetCompressionIndex(physical_type);
	if (is_loaded[index]) {
		return CompressionFunctionSetLoadResult::ALREADY_LOADED_BEFORE_LOCK;
	}
	// not loaded - try to load it
	lock_guard<mutex> guard(lock);
	// verify nobody loaded it in the mean-time
	if (is_loaded[index]) {
		return CompressionFunctionSetLoadResult::ALREADY_LOADED_AFTER_LOCK;
	}
	// actually perform the load
	auto &function_list = functions[index];
	for (idx_t i = 0; internal_compression_methods[i].get_function; i++) {
		auto &method = internal_compression_methods[i];
		if (!method.supports_type(physical_type)) {
			// not supported for this type
			continue;
		}
		// The type is supported. We create the function and insert it into the set of available functions.
		function_list.push_back(method.get_function(physical_type));
	}
	is_loaded[index] = true;
	return CompressionFunctionSetLoadResult::LAZILY_LOADED;
}

pair<CompressionFunctionSetLoadResult, optional_ptr<const CompressionFunction>>
CompressionFunctionSet::GetCompressionFunction(CompressionType type, const PhysicalType physical_type) {
	const auto load_result = LoadCompressionFunctions(physical_type);

	const auto index = GetCompressionIndex(physical_type);
	const auto &function_list = functions[index];
	for (auto &function : function_list) {
		if (function.type == type) {
			return make_pair(load_result, &function);
		}
	}
	return make_pair(load_result, nullptr);
}

void CompressionFunctionSet::SetDisabledCompressionMethods(const vector<CompressionType> &methods) {
	ResetDisabledMethods();
	for (auto &method : methods) {
		auto idx = GetCompressionIndex(method);
		is_disabled[idx] = true;
	}
}

void DBConfig::SetDisabledCompressionMethods(const vector<CompressionType> &methods) {
	compression_functions->SetDisabledCompressionMethods(methods);
}

vector<CompressionType> CompressionFunctionSet::GetDisabledCompressionMethods() const {
	vector<CompressionType> result;
	for (idx_t i = 0; i < COMPRESSION_TYPE_COUNT; i++) {
		if (is_disabled[i]) {
			result.push_back(static_cast<CompressionType>(i));
		}
	}
	return result;
}

string CompressionFunctionSet::GetDebugInfo() const {
	static PhysicalType physical_types[PHYSICAL_TYPE_COUNT] = {
	    PhysicalType::BOOL,    PhysicalType::UINT8,    PhysicalType::INT8,   PhysicalType::UINT16, PhysicalType::INT16,
	    PhysicalType::UINT32,  PhysicalType::INT32,    PhysicalType::UINT64, PhysicalType::INT64,  PhysicalType::FLOAT,
	    PhysicalType::DOUBLE,  PhysicalType::INTERVAL, PhysicalType::LIST,   PhysicalType::STRUCT, PhysicalType::ARRAY,
	    PhysicalType::VARCHAR, PhysicalType::UINT128,  PhysicalType::INT128, PhysicalType::BIT,
	};

	vector<string> compression_type_debug_infos;
	for (idx_t i = 0; i < COMPRESSION_TYPE_COUNT; i++) {
		compression_type_debug_infos.push_back(
		    StringUtil::Format("%llu: {compression type: %s, is disabled: %llu}", i,
		                       EnumUtil::ToString(internal_compression_methods[i].type), is_disabled[i].load()));
	}

	lock_guard<mutex> guard(lock);
	vector<string> physical_type_debug_infos;
	for (idx_t index = 0; index < PHYSICAL_TYPE_COUNT; index++) {
		const auto &physical_type = physical_types[index];
		D_ASSERT(GetCompressionIndex(physical_type) == index);

		idx_t out_of = 0;
		for (idx_t i = 0; internal_compression_methods[i].get_function; i++) {
			auto &method = internal_compression_methods[i];
			if (method.supports_type(physical_type)) {
				out_of++;
			}
		}

		const auto &function_list = functions[index];
		vector<string> function_list_debug_infos;
		for (idx_t function_index = 0; function_index < function_list.size(); function_index++) {
			auto &function = function_list[function_index];
			function_list_debug_infos.push_back(StringUtil::Format("%llu: {compression type: %s, physical type: %s}",
			                                                       function_index, EnumUtil::ToString(function.type),
			                                                       EnumUtil::ToString(function.data_type)));
		}

		physical_type_debug_infos.push_back(StringUtil::Format(
		    "%llu: {physical type: %s, loaded: %llu, loaded functions: %llu (out of: %llu)}\t\t%s", index,
		    EnumUtil::ToString(physical_type), is_loaded[index].load(), function_list.size(), out_of,
		    function_list_debug_infos.empty() ? "" : "\n\t\t" + StringUtil::Join(function_list_debug_infos, "\n\t\t")));
	}

	return StringUtil::Format("DEBUG INFO:\n - Compression types:\n\t%s\n\n - Physical types:\n\t%s",
	                          StringUtil::Join(compression_type_debug_infos, "\n\t"),
	                          StringUtil::Join(physical_type_debug_infos, "\n\t"));
}

vector<CompressionType> DBConfig::GetDisabledCompressionMethods() const {
	return compression_functions->GetDisabledCompressionMethods();
}

void CompressionFunctionSet::ResetDisabledMethods() {
	for (idx_t i = 0; i < COMPRESSION_TYPE_COUNT; i++) {
		is_disabled[i] = false;
	}
}

vector<reference<const CompressionFunction>> DBConfig::GetCompressionFunctions(const PhysicalType physical_type) const {
	return compression_functions->GetCompressionFunctions(physical_type);
}

optional_ptr<const CompressionFunction> DBConfig::TryGetCompressionFunction(CompressionType type,
                                                                            const PhysicalType physical_type) const {
	return compression_functions->GetCompressionFunction(type, physical_type).second;
}

reference<const CompressionFunction> DBConfig::GetCompressionFunction(CompressionType type,
                                                                      const PhysicalType physical_type) const {
	auto result = compression_functions->GetCompressionFunction(type, physical_type);
	if (!result.second) {
		throw InternalException(
		    "Could not find compression function \"%s\" for physical type \"%s\". Load result: %s. %s",
		    EnumUtil::ToString(type), EnumUtil::ToString(physical_type), EnumUtil::ToString(result.first),
		    compression_functions->GetDebugInfo());
	}
	return *result.second;
}
} // namespace duckdb
