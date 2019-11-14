#pragma once

#define MD5_HASH_LENGTH 64

/*
 * If compiled on a machine that doesn't have a 32-bit integer,
 * you just set "uint32" to the appropriate datatype for an
 * unsigned 32-bit integer.  For example:
 *
 *       cc -Duint32='unsigned long' md5.c
 *
 */
#ifndef uint32
#define uint32 unsigned int
#endif

struct Context {
	int isInit;
	uint32 buf[4];
	uint32 bits[2];
	unsigned char in[64];
};
typedef struct Context MD5Context;

void md5_init(MD5Context *context);
void md5_add(MD5Context *context, const char *z);
void md5_finish(MD5Context *context, char zResult[]);
