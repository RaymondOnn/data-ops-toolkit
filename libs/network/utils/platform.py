"""Platform detection and system utilities."""

import platform
import subprocess


class PlatformUtils:
    """Platform detection and command utilities."""

    @staticmethod
    def is_windows() -> bool:
        """Check if running on Windows."""
        return platform.system() == "Windows"

    @staticmethod
    def is_linux() -> bool:
        """Check if running on Linux."""
        return platform.system() == "Linux"

    @staticmethod
    def is_macos() -> bool:
        """Check if running on macOS."""
        return platform.system() == "Darwin"

    @staticmethod
    def get_os_name() -> str:
        """Get OS name."""
        return platform.system()

    @staticmethod
    def get_os_release() -> str:
        """Get OS release version."""
        return platform.release()

    @staticmethod
    def command_exists(cmd: str) -> bool:
        """Check if command exists in PATH."""
        try:
            which_cmd = "where" if PlatformUtils.is_windows() else "which"
            subprocess.run(
                [which_cmd, cmd],
                capture_output=True,
                check=True,
            )
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False
