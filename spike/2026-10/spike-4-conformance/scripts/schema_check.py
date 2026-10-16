#!/usr/bin/env python3
"""Spike 4 — 用官方 Serverless Workflow 1.0.3 JSON Schema 校验语料。

用法：schema_check.py <schema.yaml> <corpus-dir> > schema-results.json
依赖：PyYAML、jsonschema（Draft 2020-12）。
"""

import glob
import json
import os
import sys

import yaml
from jsonschema import Draft202012Validator


def main() -> int:
    schema_path, corpus_dir = sys.argv[1], sys.argv[2]
    schema = yaml.safe_load(open(schema_path, encoding="utf-8"))
    validator = Draft202012Validator(schema)

    results = []
    for path in sorted(glob.glob(os.path.join(corpus_dir, "*.yaml"))):
        name = os.path.basename(path)
        try:
            doc = yaml.safe_load(open(path, encoding="utf-8"))
        except yaml.YAMLError as exc:
            results.append({"file": name, "schema_ok": False, "errors": [f"yaml: {exc}"]})
            continue
        errors = sorted(validator.iter_errors(doc), key=lambda e: list(e.path))
        results.append(
            {
                "file": name,
                "schema_ok": not errors,
                "errors": [f"{'/'.join(str(x) for x in e.path)}: {e.message}" for e in errors[:5]],
                "dsl": (doc or {}).get("document", {}).get("dsl") if isinstance(doc, dict) else None,
            }
        )

    print(json.dumps({"tool": "schema_check", "schema": schema_path, "results": results}, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
