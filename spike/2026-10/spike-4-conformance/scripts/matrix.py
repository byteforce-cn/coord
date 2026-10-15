#!/usr/bin/env python3
"""Spike 4 — 合并 schema 校验与 coord 解析器结果，输出对照矩阵。

用法：matrix.py <schema-results.json> <parser-results.json> <out.tsv>
分类：
  PASS      官方合法 & coord 接受
  GAP       官方合法 & coord 拒绝     （规范符合性缺口）
  LAX       coord 接受 & 官方不合法   （宽松/行为不一致）
  REJECTED  双方都拒绝
"""

import json
import sys

FIELDS = "真实结论\t文件\tschema\tparser\troute\trun_kinds\textensions\t错误信息"


def main() -> int:
    schema = json.load(open(sys.argv[1], encoding="utf-8"))["results"]
    parser = json.load(open(sys.argv[2], encoding="utf-8"))["results"]
    out_path = sys.argv[3]

    schema_by = {r["file"]: r for r in schema}
    parser_by = {r["file"]: r for r in parser}
    files = sorted(set(schema_by) | set(parser_by))

    rows = []
    counts = {"PASS": 0, "GAP": 0, "LAX": 0, "REJECTED": 0}
    for f in files:
        s = schema_by.get(f, {})
        p = parser_by.get(f, {})
        schema_ok = bool(s.get("schema_ok"))
        parser_ok = bool(p.get("parser_ok"))
        if schema_ok and parser_ok:
            verdict = "PASS"
        elif schema_ok and not parser_ok:
            verdict = "GAP"
        elif parser_ok and not schema_ok:
            verdict = "LAX"
        else:
            verdict = "REJECTED"
        counts[verdict] += 1
        errs = " | ".join(p.get("errors", [])) or " | ".join(s.get("errors", []))
        rows.append(
            [
                verdict,
                f,
                "OK" if schema_ok else "FAIL",
                "OK" if parser_ok else "FAIL",
                p.get("route", "-"),
                ",".join(p.get("run_kinds", [])) or "-",
                "yes" if p.get("has_extensions") else "no",
                errs.replace("\t", " ").replace("\n", " "),
            ]
        )

    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(FIELDS + "\n")
        for r in rows:
            fh.write("\t".join(r) + "\n")

    print(json.dumps({"counts": counts, "out": out_path}, ensure_ascii=False))
    for verdict in ("GAP", "LAX"):
        print(f"== {verdict} ==")
        for r in rows:
            if r[0] == verdict:
                print(f"  {r[1]}: {r[7][:160]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
