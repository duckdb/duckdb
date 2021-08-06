#pragma once

#ifdef _WIN32

#ifndef NOMINMAX
#define NOMINMAX
#endif

#include <windows.h>

#undef CreateDirectory
#undef MoveFile
#undef RemoveDirectory

#endif