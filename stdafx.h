// stdafx.h : include file for standard system include files,
// or project specific include files that are used frequently, but
// are changed infrequently
//

#pragma once

#include "targetver.h"

#include <stdio.h>
#include <tchar.h>
#include <Windows.h>
#include <time.h>
#include <vector>
#include <string>
#include <sstream>


#include <XMySQLLib/XMySQLLib.h>


#ifdef _UNICODE
	#define STDSTRINGSTREAM		std::wstringstream
	#define STDSTRING					std::wstring
#else		// _UNICODE
	#define STDSTRINGSTREAM		std::stringstream
	#define STDSTRING					std::string
#endif	// _UNICODE