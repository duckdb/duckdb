import generate_connection_methods
import generate_connection_stubs
import generate_connection_wrapper_methods
import generate_connection_wrapper_stubs

if __name__ == '__main__':
    generate_connection_methods.generate()
    generate_connection_stubs.generate()
    generate_connection_wrapper_methods.generate()
    generate_connection_wrapper_stubs.generate()
