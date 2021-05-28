#pragma once

#include "duckdb/common/types.hpp"

using namespace duckdb;

struct sqlite3_value {
    union MemValue {
        double r;           /* Real value used when MEM_Real is set in flags */
        int64_t i;          /* Integer value used when MEM_Int is set in flags */
        int nZero;          /* Extra zero bytes when MEM_Zero and MEM_Blob set */
        const char *zPType; /* Pointer type when MEM_Term|MEM_Subtype|MEM_Null */
    } u;
    // uint16_t flags;
    PhysicalType type;
};

struct sqlite3_context {
    sqlite3_value result;
//   Mem *pOut;              /* The return value is stored here */
//   FuncDef *pFunc;         /* Pointer to function information */
//   Mem *pMem;              /* Memory cell used to store aggregate context */
//   Vdbe *pVdbe;            /* The VM that owns this context */
//   int iOp;                /* Instruction number of OP_Function */
//   int isError;            /* Error code returned by the function. */
//   u8 skipFlag;            /* Skip accumulator loading if true */
//   u8 argc;                /* Number of arguments */
//   sqlite3_value *argv[1]; /* Argument set */
};
