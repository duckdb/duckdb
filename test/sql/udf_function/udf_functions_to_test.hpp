/*HEADER file with all UDF Functions to test*/
#include "duckdb/common/types/string_type.hpp"

#pragma once
using namespace duckdb;

//UDF Functions to test
bool BOOL(bool a) {return a;}
bool BOOL(bool a, bool b) {return a & b;}
bool BOOL(bool a, bool b, bool c) {return a & b & c;}

int8_t INT8(int8_t a) {return a;}
int8_t INT8(int8_t a, int8_t b) {return a * b;}
int8_t INT8(int8_t a, int8_t b, int8_t c) {return a + b + c;}

int16_t INT16(int16_t a) {return a;}
int16_t INT16(int16_t a, int16_t b) {return a * b;}
int16_t INT16(int16_t a, int16_t b, int16_t c) {return a + b + c;}

int DATE(int a) {return a;}
int DATE(int a, int b) {return b;}
int DATE(int a, int b, int c) {return c;}

int TIME(int a) {return a;}
int TIME(int a, int b) {return b;}
int TIME(int a, int b, int c) {return c;}

int INT(int a) {return a;}
int INT(int a, int b) {return a * b;}
int INT(int a, int b, int c) {return a + b + c;}

int64_t INT64(int64_t a) {return a;}
int64_t INT64(int64_t a, int64_t b) {return a * b;}
int64_t INT64(int64_t a, int64_t b, int64_t c) {return a + b + c;}

int64_t TIMESTAMP(int64_t a) {return a;}
int64_t TIMESTAMP(int64_t a, int64_t b) {return b;}
int64_t TIMESTAMP(int64_t a, int64_t b, int64_t c) {return c;}

float FLOAT(float a) {return a;}
float FLOAT(float a, float b) {return a * b;}
float FLOAT(float a, float b, float c) {return a + b + c;}

double DOUBLE(double a) {return a;}
double DOUBLE(double a, double b) {return a * b;}
double DOUBLE(double a, double b, double c) {return a + b + c;}

double DECIMAL(double a) {return a;}
double DECIMAL(double a, double b) {return a * b;}
double DECIMAL(double a, double b, double c) {return a + b + c;}

string_t VARCHAR(string_t a) {return a;}
string_t VARCHAR(string_t a, string_t b) {return b;}
string_t VARCHAR(string_t a, string_t b, string_t c) {return c;}
