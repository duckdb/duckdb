#include "cpu_arch.hpp"

namespace duckdb {
string ArchitectureToString(Architecture arch) {
	switch (arch) {
	case Architecture::FALLBACK:
		return "FALLBACK";
	case Architecture::X86:
		return "X86";
	case Architecture::X86_64:
		return "X86_64";
	case Architecture::ARM:
		return "ARM";
	default:
		return "UNDEFINED";
	}
}
}