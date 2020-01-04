import argparse
import sys


def parse_args(args=None):
    parser = argparse.ArgumentParser(description='Define the working directories.')
    """"""
    parser.add_argument("--work_dir", help="work dir", default="/Users/eric/spark-tutorial")
    args = sys.argv[1:] if args is None else args
    return parser.parse_args(args)
