#include <cmath>
#include <stdio.h>
#include <dlfcn.h>

extern "C" {

int even(int x)
{
    double value;
    if (x >= 0) {
        value = std::ceil(x);
    } else {
        value = std::ceil(-x);
        value = -value;
    }
    if (std::floor(value / 2) * 2 != value) {
        if (x >= 0) {
            return value += 1;
        }
        return value -= 1;
    }
    return (int)value;
}
}