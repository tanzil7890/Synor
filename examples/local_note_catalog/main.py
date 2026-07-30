import json
import pathlib

import synor as syn
from synor.connectors import localfs
from synor.resources.file import FileLike, PatternFilePathMatcher


def _note_record(text: str) -> dict[str, object]:
    lines = [line.strip() for line in text.splitlines()]
    title = next(
        (line.removeprefix("# ").strip() for line in lines if line.startswith("# ")),
        "Untitled note",
    )
    tags_line = next(
        (
            line.removeprefix("tags:").strip()
            for line in lines
            if line.startswith("tags:")
        ),
        "",
    )
    return {
        "title": title,
        "tags": [tag.strip() for tag in tags_line.split(",") if tag.strip()],
        "word_count": len(text.split()),
    }


@syn.fn(memo=True)
async def catalog_note(file: FileLike, catalog_dir: pathlib.Path) -> None:
    record = _note_record(await file.read_text())
    localfs.declare_file(
        catalog_dir / f"{file.file_path.path.stem}.json",
        json.dumps(record, indent=2) + "\n",
        create_parent_dirs=True,
    )


@syn.fn
async def app_main(notes_dir: pathlib.Path, catalog_dir: pathlib.Path) -> None:
    notes = localfs.walk_dir(
        notes_dir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
    )
    await syn.mount_each(catalog_note, notes.items(), catalog_dir)


app = syn.App(
    syn.AppConfig(name="LocalNoteCatalog"),
    app_main,
    notes_dir=pathlib.Path("./notes"),
    catalog_dir=pathlib.Path("./catalog"),
)
