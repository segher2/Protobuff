from pathlib import Path
import subprocess
import shutil


def main() -> None:
    # repo_root / bench_out
    # repo_root = Path(__file__).resolve().parents[1]
    # bench_out = repo_root / "bench_out"
    # bench_out.mkdir(exist_ok=True)

    repo_root = Path(__file__).resolve().parents[1]
    data = repo_root / "examples"/ "data"
    data.mkdir(exist_ok=True)


    # Input/Output (change names if you like)
    input_geojson = data / "bag_pand_50k.geojson"
    output_fgb = data / "out_bag_pand_50k.fgb"

    if not input_geojson.exists():
        raise FileNotFoundError(f"Missing input: {input_geojson}")

    # Find ogr2ogr
    ogr2ogr = shutil.which("ogr2ogr")
    if ogr2ogr is None:
        raise RuntimeError(
            "ogr2ogr not found.\n"
            "Activate a conda env with GDAL installed, e.g.:\n"
            "  conda install -c conda-forge gdal"
        )

    # Convert GeoJSON -> FlatGeobuf
    subprocess.run(
        [
            ogr2ogr,
            "-f", "FlatGeobuf",
            str(output_fgb),
            str(input_geojson),
        ],
        check=True,
    )

    print("✅ Wrote:", output_fgb)


if __name__ == "__main__":
    main()
