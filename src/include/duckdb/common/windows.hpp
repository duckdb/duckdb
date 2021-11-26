#pragma once

#ifdef _WIN32

#ifndef NOMINMAX
#define NOMINMAX
#endif

#include <winsock2.h>
#include <windows.h>

#undef CreateDirectory
#undef MoveFile
#undef RemoveDirectory

#endif