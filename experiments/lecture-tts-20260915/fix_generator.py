#!/usr/bin/env python3
from pathlib import Path
import py_compile
import sys

p = Path(sys.argv[1])
lines = p.read_text(encoding="utf-8").splitlines()
idxs = [i for i, line in enumerate(lines) if 'concat.write_text("".join' in line]
if len(idxs) != 1:
    raise RuntimeError(f"expected one concat line, found {len(idxs)}")
i = idxs[0]
replacement = [
    '    concat_lines = []',
    '    for c in chunks:',
    '        chunk_path = chunks_dir / f"{c[\'index\']:03d}.wav"',
    '        concat_lines.append(f"file \'{chunk_path.as_posix()}\'\\n")',
    '    concat.write_text("".join(concat_lines), encoding="utf-8")',
]
lines[i:i+1] = replacement
p.write_text("\n".join(lines) + "\n", encoding="utf-8")
py_compile.compile(str(p), doraise=True)
print("patched and compiled", p)
