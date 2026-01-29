import argparse
import json
from pathlib import Path
import sys

from sfproto.geojson.api import encode_geojson, decode_geojson


def read_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, obj):
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)


def read_bytes(path: Path) -> bytes:
    return path.read_bytes()


def write_bytes(path: Path, data: bytes):
    path.write_bytes(data)


def cmd_encode(args):
    geojson = read_json(args.input)
    data = encode_geojson(geojson, delta=args.delta)

    if args.output:
        write_bytes(args.output, data)
    else:
        sys.stdout.buffer.write(data)


def cmd_decode(args):
    data = read_bytes(args.input)
    geojson = decode_geojson(data, delta=args.delta)

    if args.output:
        write_json(args.output, geojson)
    else:
        json.dump(geojson, sys.stdout, indent=2)


def main():
    parser = argparse.ArgumentParser(prog="sfproto")
    subparsers = parser.add_subparsers(required=True)

    # encode
    encode = subparsers.add_parser("encode", help="Encode GeoJSON to protobuf")
    encode.add_argument("input", type=Path)
    encode.add_argument("-o", "--output", type=Path)
    encode.add_argument(
        "--delta",
        action="store_true",
        help="Use delta encoding (v7). Default is non-delta (v4).",
    )
    encode.set_defaults(func=cmd_encode)

    # decode
    decode = subparsers.add_parser("decode", help="Decode protobuf to GeoJSON")
    decode.add_argument("input", type=Path)
    decode.add_argument("-o", "--output", type=Path)
    decode.add_argument(
        "--delta",
        action="store_true",
        help="Decode using delta encoding (v7).",
    )
    decode.set_defaults(func=cmd_decode)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
