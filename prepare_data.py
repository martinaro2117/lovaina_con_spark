"""
prepare_data.py
───────────────
Convierte cache-0-json.gz (formato JSON array de tweets) al formato
JSONL (un tweet por línea) que spark.read.json() puede consumir en paralelo.

Uso:
    python prepare_data.py
    python prepare_data.py --input data/cache-0-json.gz --output data/tweets.jsonl

Requisitos: ninguno fuera de la stdlib (gzip, json, argparse).
"""

import gzip
import json
import argparse
from pathlib import Path


def convert(gz_path: Path, jsonl_path: Path, max_tweets: int = 0) -> int:
    """
    Lee el archivo .gz línea a línea y escribe cada tweet como una
    línea JSON independiente en el archivo de destino.

    Se hace streaming (sin cargar todo en memoria) para soportar
    datasets de varios GB.

    Devuelve el número de tweets escritos.
    """
    written = 0

    with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace") as f_in, \
         open(jsonl_path, "w", encoding="utf-8") as f_out:

        for raw_line in f_in:
            line = raw_line.strip().rstrip(",")

            # Saltar líneas vacías o que sean sólo corchetes del array JSON
            if not line or line in ("[", "]"):
                continue

            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            # Sólo tweets (tienen id_str o id)
            if not isinstance(obj, dict):
                continue
            if "id_str" not in obj and "id" not in obj:
                continue

            f_out.write(json.dumps(obj, ensure_ascii=False) + "\n")
            written += 1

            if max_tweets and written >= max_tweets:
                break

            if written % 50_000 == 0:
                print(f"  {written:,} tweets procesados…")

    return written


def main():
    parser = argparse.ArgumentParser(description="Convierte cache-0-json.gz → tweets.jsonl")
    parser.add_argument("--input",  default="data/cache-0-json.gz", help="Ruta al .gz")
    parser.add_argument("--output", default="data/tweets.jsonl",    help="Ruta de salida JSONL")
    parser.add_argument("--max",    type=int, default=0,
                        help="Límite de tweets (0 = todos, útil para pruebas)")
    args = parser.parse_args()

    gz_path   = Path(args.input)
    jsonl_path = Path(args.output)

    if not gz_path.exists():
        print(f"✗ No se encuentra el archivo: {gz_path}")
        raise SystemExit(1)

    jsonl_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Convirtiendo {gz_path} → {jsonl_path} …")
    n = convert(gz_path, jsonl_path, max_tweets=args.max)
    size_mb = jsonl_path.stat().st_size / 1e6
    print(f"✓ {n:,} tweets escritos ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
