#pragma once

#if defined(_WIN32)

#ifndef NOMINMAX
#define NOMINMAX
#endif

#ifndef _WINSOCKAPI_
#define _WINSOCKAPI_
#endif

#include <windows.h>

#undef CreateDirectory
#undef MoveFile
// start Anybase changes
#undef CopyFile
// end Anybase changes
#undef RemoveDirectory

#endif
