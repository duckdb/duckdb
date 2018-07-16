
#include <stdio.h>

#include "common/printable.hpp"

void Printable::Print() { fprintf(stderr, "%s\n", ToString().c_str()); }
