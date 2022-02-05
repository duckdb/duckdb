#include "rapi.hpp"
#include "altrepstring.hpp"
#include <R_ext/Rdynload.h>

// When changing this file, run cpp11::cpp_register() from R

// exception required as long as r-lib/decor#6 remains
// clang-format off
[[cpp11::init]] void AltrepString_Initialize(DllInfo* dll) {
	// clang-format on
	AltrepString::Initialize(dll);
}
