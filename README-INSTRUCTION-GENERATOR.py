#!/usr/bin/env python3
"""
README.md Generator — Interactive CLI
Reads a Python script + CLAUDE.md instructions, then uses the Claude API
to generate a structured README.md file written to disk.
"""

import os
import sys
import argparse
import anthropic
from pathlib import Path
from datetime import datetime

# ── Default CLAUDE.md content (used when no CLAUDE.md file is found) ─────────
DEFAULT_CLAUDE_MD = """
# CLAUDE.md — README Generation Instructions

You are a technical documentation agent. Your task is to generate a clear,
professional README.md for the provided Python script.

## README Structure (follow exactly)

1. **Title** — Script name as H1 heading
2. **Description** — 2–3 sentence summary of what the script does
3. **Features** — Bullet list of key capabilities
4. **Requirements** — Python version and pip dependencies
5. **Installation** — Step-by-step setup commands
6. **Usage** — How to run the script with example commands
7. **Configuration** — Any env variables, config files, or CLI flags
8. **Output** — What the script produces (files, logs, etc.)
9. **Notes** — Edge cases, limitations, or developer tips

## Rules
- Use GitHub-flavored Markdown
- Keep language concise and technical
- Include code blocks for all commands and code snippets
- Do NOT invent features not present in the script
- Do NOT include a License section unless the script has one
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_claude_md(script_dir: Path) -> tuple[str, str]:
    """Look for CLAUDE.md next to the target script. Fall back to default."""
    candidates = [
        script_dir / "CLAUDE.md",
        Path.cwd() / "CLAUDE.md",
    ]
    for path in candidates:
        if path.exists():
            content = path.read_text(encoding="utf-8")
            return content, str(path)
    return DEFAULT_CLAUDE_MD, "(built-in default)"


def read_python_script(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except Exception as e:
        print(f"  Could not read script: {e}")
        sys.exit(1)


def prompt_for_script() -> Path:
    """Interactively ask the user for a Python script path."""
    while True:
        raw = input("  Enter path to Python script: ").strip()
        if not raw:
            print("  Path cannot be empty.")
            continue
        p = Path(raw).expanduser().resolve()
        if not p.exists():
            print(f"  File not found: {p}")
            continue
        if p.suffix not in (".py", ""):
            ans = input(f"   '{p.name}' doesn't look like a .py file. Continue? [y/N] ")
            if ans.strip().lower() != "y":
                continue
        return p


def prompt_for_output(script_path: Path) -> Path:
    """Ask where to write README.md (default: same folder as script)."""
    default = script_path.parent / "README.md"
    raw = input(f"  Output path [{default}]: ").strip()
    if not raw:
        return default
    p = Path(raw).expanduser().resolve()
    if p.is_dir():
        return p / "README.md"
    return p


def generate_readme(system_prompt: str, script_code: str, script_name: str) -> str:
    """Call the Claude API and return the generated README content."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(api_key=api_key)

    user_message = (
        f"Generate a README.md for the following Python script named `{script_name}`.\n\n"
        f"```python\n{script_code}\n```\n\n"
        "Return ONLY the raw Markdown content — no preamble, no explanation."
    )

    print("\n  ⏳ Calling Claude API …")
    message = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=2048,
        system=system_prompt,
        messages=[{"role": "user", "content": user_message}],
    )
    return message.content[0].text


def write_readme(content: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")



# ── Main interactive flow ─────────────────────────────────────────────────────

def prompt_for_folder() -> Path:
    """Interactively ask the user for a folder path."""
    while True:
        raw = input("  Enter folder path (or press Enter for current directory): ").strip()
        if not raw:
            return Path.cwd()
        p = Path(raw).expanduser().resolve()
        if not p.exists():
            print(f"  ✗ Folder not found: {p}")
            continue
        if not p.is_dir():
            print(f"  ✗ That path is a file, not a folder: {p}")
            continue
        return p


def main():
    parser = argparse.ArgumentParser(
        description="Generate README-{name}.md for every .py file in a folder using Claude."
    )
    parser.add_argument("folder", nargs="?", help="Folder to scan for .py files (optional — will prompt if omitted)")
    parser.add_argument("--claude-md", help="Explicit path to a CLAUDE.md instructions file")
    args = parser.parse_args()

    print(BANNER)

    # ── Step 1: Resolve target folder ─────────────────────────────────────────
    print("Step 1 — Target Folder")
    if args.folder:
        root = Path(args.folder).expanduser().resolve()
        if not root.is_dir():
            print(f"  Folder not found: {root}")
            sys.exit(1)
    else:
        root = prompt_for_folder()
    print(f"  Scanning: {root}")

    # ── Step 2: Load CLAUDE.md instructions ───────────────────────────────────
    print("\n Step 2 — CLAUDE.md Instructions")
    if args.claude_md:
        claude_path = Path(args.claude_md).expanduser().resolve()
        if not claude_path.exists():
            print(f" CLAUDE.md not found at: {claude_path}")
            sys.exit(1)
        system_prompt = claude_path.read_text(encoding="utf-8")
        source_label = str(claude_path)
    else:
        system_prompt, source_label = load_claude_md(root)
    print(f"  Instructions loaded from: {source_label}")

    # ── Step 3: Find .py files ─────────────────────────────────────────────────
    print("\n Step 3 — Discovering Python Files")
    self_path = Path(__file__).resolve()
    py_files = sorted(p for p in root.rglob("*.py") if p.resolve() != self_path)

    if not py_files:
        print(" No .py files found in that folder.")
        sys.exit(1)

    print(f"  Found {len(py_files)} file(s):\n")
    for i, p in enumerate(py_files, 1):
        print(f"    {i:>3}. {p.relative_to(root)}")

    # ── Step 4: Confirm and generate ──────────────────────────────────────────
    ans = input(f"\n  Generate README-{{name}}.md for all {len(py_files)} files? [Y/n] ").strip().lower()
    if ans == "n":
        print("  Aborted.")
        sys.exit(0)

    print()
    success, failed = 0, []
    for script_path in py_files:
        output_path = script_path.parent / f"README-{script_path.stem}.md"
        rel = script_path.relative_to(root)
        print(f"  {rel} …", end=" ", flush=True)
        try:
            code = read_python_script(script_path)
            content = generate_readme(system_prompt, code, script_path.name)
            write_readme(content, output_path)
            lines = content.count("\n") + 1
            print(f" ({lines} lines → {output_path.name})")
            success += 1
        except Exception as e:
            print(f"  {e}")
            failed.append((rel, str(e)))

    # ── Done ──────────────────────────────────────────────────────────────────
    print(f"\n  Done — {success} README(s) generated, {len(failed)} failed.")
    if failed:
        print("  Failed files:")
        for rel, err in failed:
            print(f"    - {rel}: {err}")
    print(f"  Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")


if __name__ == "__main__":
    main()