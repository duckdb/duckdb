from sqllogictest import SQLParserException, SQLLogicParser, SQLLogicTest

from typing import Optional
import argparse


def main():
    parser = argparse.ArgumentParser(description="SQL Logic Parser")
    parser.add_argument("filename", type=str, help="Path to the SQL logic file")
    args = parser.parse_args()

    filename = args.filename

    parser = SQLLogicParser()
    out: Optional[SQLLogicTest] = parser.parse(filename)
    if not out:
        raise SQLParserException(f"Test {filename} could not be parsed")


if __name__ == "__main__":
    main()
