//---------------------------------------------------------------------------
//	@filename:
//		utils.cpp
//
//	@doc:
//		Various utilities which are not necessarily gpos specific
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/utils.h"
#include "duckdb/optimizer/cascade/types.h"
#include <assert.h>

// using 16 addresses a line fits exactly into 80 characters
#define GPOS_MEM_BPL 16

// number of stack frames to search for addresses
#define GPOS_SEARCH_STACK_FRAMES 16

using namespace gpos;

//---------------------------------------------------------------------------
//	GPOS versions of standard streams
//	Do not reference std::(w)cerr/(w)cout directly;
//---------------------------------------------------------------------------
COstreamBasic gpos::oswcerr(&std::wcerr);
COstreamBasic gpos::oswcout(&std::wcout);

//---------------------------------------------------------------------------
//	@function:
//		gpos::Print
//
//	@doc:
//		Print wide-character string
//
//---------------------------------------------------------------------------
void gpos::Print(WCHAR *wsz)
{
	std::wcout << wsz;
}

//---------------------------------------------------------------------------
//	@function:
//		gpos::HexDump
//
//	@doc:
//		Generic memory dumper; produces regular hex dump
//
//---------------------------------------------------------------------------
IOstream & gpos::HexDump(IOstream &os, const void *pv, ULLONG size)
{
	for (ULONG i = 0; i < 1 + (size / GPOS_MEM_BPL); i++)
	{
		// starting address of line
		BYTE *buf = ((BYTE *) pv) + (GPOS_MEM_BPL * i);
		os << (void *) buf << "  ";
		os << COstream::EsmHex;
		// individual bytes
		for (ULONG j = 0; j < GPOS_MEM_BPL; j++)
		{
			if (buf[j] < 16)
			{
				os << "0";
			}
			os << (ULONG) buf[j] << " ";
			// separator in middle of line
			if (j + 1 == GPOS_MEM_BPL / 2)
			{
				os << "- ";
			}
		}
		// blank between hex and text dump
		os << " ";
		// text representation
		for (ULONG j = 0; j < GPOS_MEM_BPL; j++)
		{
			// print only 'visible' characters
			if (buf[j] >= 0x20 && buf[j] <= 0x7f)
			{
				// cast to CHAR to avoid stream from (mis-)interpreting BYTE
				os << (CHAR) buf[j];
			}
			else
			{
				os << ".";
			}
		}
		os << COstream::EsmDec << std::endl;
	}
	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		gpos::HashByteArray
//
//	@doc:
//		Generic hash function for an array of BYTEs
//		Taken from D. E. Knuth;
//
//---------------------------------------------------------------------------
ULONG gpos::HashByteArray(const BYTE *byte_array, ULONG size)
{
	ULONG hash = size;
	for (ULONG i = 0; i < size; ++i)
	{
		BYTE b = byte_array[i];
		hash = ((hash << 5) ^ (hash >> 27)) ^ b;
	}
	return hash;
}

//---------------------------------------------------------------------------
//	@function:
//		gpos::CombineHashes
//
//	@doc:
//		Combine ULONG-based hash values
//
//---------------------------------------------------------------------------
ULONG gpos::CombineHashes(ULONG hash1, ULONG hash2)
{
	ULONG hashes[2];
	hashes[0] = hash1;
	hashes[1] = hash2;
	return HashByteArray((BYTE *) hashes, GPOS_ARRAY_SIZE(hashes) * sizeof(hashes[0]));
}


//---------------------------------------------------------------------------
//	@function:
//		gpos::Add
//
//	@doc:
//		Add two unsigned long long values, throw an exception if overflow occurs,
//
//---------------------------------------------------------------------------
ULLONG gpos::Add(ULLONG first, ULLONG second)
{
	if (first > gpos::ullong_max - second)
	{
		// if addition result overflows, we have (a + b > gpos::ullong_max),
		// then we need to check for  (a > gpos::ullong_max - b)
		assert(false);
	}
	ULLONG res = first + second;
	return res;
}

//---------------------------------------------------------------------------
//	@function:
//		gpos::Multiply
//
//	@doc:
//		Multiply two unsigned long long values, throw an exception if overflow occurs,
//
//---------------------------------------------------------------------------
ULLONG gpos::Multiply(ULLONG first, ULLONG second)
{
	if (0 < second && first > gpos::ullong_max / second)
	{
		// if multiplication result overflows, we have (a * b > gpos::ullong_max),
		// then we need to check for  (a > gpos::ullong_max / b)
		assert(false);
	}
	ULLONG res = first * second;
	return res;
}