class IndentedFileWriter:
    """Wrapper around a file object that adds write_indented method."""

    def __init__(self, file_path, read_only=False):
        mode = 'r' if read_only else 'w'
        self.file = open(file_path, mode, encoding='utf-8', newline='\n')

    def write_indented(self, indent_level, text):
        """Write text to file with the specified indentation level."""
        indent = '\t' * indent_level
        self.file.write(f"{indent}{text}\n")

    def write(self, text):
        """Delegate write to the underlying file object."""
        self.file.write(text)

    def close(self):
        """Delegate close to the underlying file object."""
        self.file.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
