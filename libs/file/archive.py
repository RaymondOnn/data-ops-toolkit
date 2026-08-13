import shutil
import tarfile
import tempfile
import zipfile
from pathlib import Path
from types import TracebackType
from typing import Any, Self


class ArchiveContext:
    """Context manager for archive inspection, selective extraction, and temp lifecycle."""

    ARCHIVE_EXTS = (".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tar.xz")
    ZIP_EXTS = (".zip",)

    def __init__(
        self,
        fs_client: Any,
        archive_path: str,
        temp_dir: str | Path | None = None,
    ):
        self.fs = fs_client
        self.archive_path = self.fs.resolve(archive_path)
        self.custom_temp_dir = temp_dir
        self.temp_dir: str | None = None
        self._is_auto_temp: bool = False

    def __enter__(self) -> Self:
        if self.custom_temp_dir:
            p = Path(self.custom_temp_dir)
            p.mkdir(parents=True, exist_ok=True)
            self.temp_dir = str(p)
            self._is_auto_temp = False
        else:
            self.temp_dir = tempfile.mkdtemp(prefix="archive_stage_")
            self._is_auto_temp = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        # Automatically clean up tempfile-generated directories
        if self._is_auto_temp and self.temp_dir and Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    # -------------------------------------------------------------------------
    # Format Detection Classmethods
    # -------------------------------------------------------------------------

    @classmethod
    def is_archive(cls, path: str) -> bool:
        p = path.lower()
        return any(p.endswith(ext) for ext in cls.ARCHIVE_EXTS) or "archive://" in p

    @classmethod
    def is_zip(cls, path: str) -> bool:
        p = path.lower()
        return any(p.endswith(ext) for ext in cls.ZIP_EXTS) or "zip://" in p

    @classmethod
    def is_supported(cls, path: str) -> bool:
        return cls.is_archive(path) or cls.is_zip(path)

    # -------------------------------------------------------------------------
    # Inspection & Extraction
    # -------------------------------------------------------------------------

    def list_contents(self) -> list[dict[str, Any]]:
        """Inspect archive metadata without extracting files."""
        contents: list[dict[str, Any]] = []

        with self.fs.open(self.archive_path, "rb") as f:
            if self.is_zip(self.archive_path):
                with zipfile.ZipFile(f) as z:
                    for info in z.infolist():
                        if not info.is_dir():
                            contents.append(
                                {"filename": info.filename, "size": info.file_size}
                            )
            else:
                with tarfile.open(fileobj=f) as t:
                    for member in t.getmembers():
                        if member.isfile():
                            contents.append(
                                {"filename": member.name, "size": member.size}
                            )

        return contents

    def extract_member(self, inner_filename: str) -> str:
        """Extract a single member file into the managed temp directory."""
        if not self.temp_dir:
            raise RuntimeError("ArchiveContext must be used as a context manager.")

        target_path = Path(self.temp_dir) / inner_filename
        target_path.parent.mkdir(exist_ok=True, parents=True)

        with self.fs.open(self.archive_path, "rb") as src_f:
            if self.is_zip(self.archive_path):
                with (
                    zipfile.ZipFile(src_f) as z,
                    z.open(inner_filename) as member,
                    target_path.open("wb") as dst_f,
                ):
                    shutil.copyfileobj(member, dst_f)
            else:
                with tarfile.open(fileobj=src_f) as t:
                    extracted_file = t.extractfile(inner_filename)
                    if extracted_file:
                        with (
                            extracted_file as member_f,
                            target_path.open("wb") as dst_f,
                        ):
                            shutil.copyfileobj(member_f, dst_f)

        return str(target_path)
