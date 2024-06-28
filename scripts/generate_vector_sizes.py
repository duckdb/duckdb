supported_vector_sizes = [2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096]

result = ""
for i in range(len(supported_vector_sizes)):
    vsize = supported_vector_sizes[i]
    if i == 0:
        result += "#if"
    else:
        result += "#elif"
    result += " STANDARD_VECTOR_SIZE == " + str(vsize) + "\n"
    result += "const sel_t FlatVector::incremental_vector[] = {"
    for idx in range(vsize):
        if idx != 0:
            result += ", "
        result += str(idx)
    result += "};\n"

result += """#else
#error Unsupported VECTOR_SIZE!
#endif"""
print(result)
