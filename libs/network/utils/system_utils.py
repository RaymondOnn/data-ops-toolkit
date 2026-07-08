"""System utility functions."""

import subprocess
import time


class SystemUtils:
    """System command execution utilities."""

    @staticmethod
    def run_command(
        command: str,
        timeout: int = 10,
        capture_output: bool = True,
    ) -> tuple[int, str, str]:
        """
        Run a system command.

        Args:
            command: Command to run.
            timeout: Timeout in seconds.
            capture_output: Whether to capture output.

        Returns:
            tuple[int, str, str]: (return_code, stdout, stderr)
        """
        try:
            result = subprocess.run(
                command,
                shell=True,
                capture_output=capture_output,
                text=True,
                timeout=timeout,
                check=False,
            )
            return result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            return -1, "", f"Command timed out after {timeout}s"
        except Exception as e:
            return -1, "", str(e)

    @staticmethod
    def retry_command(
        command: str,
        max_attempts: int = 3,
        delay: int = 1,
        timeout: int = 10,
    ) -> tuple[int, str, str]:
        """
        Retry a command multiple times.

        Args:
            command: Command to run.
            max_attempts: Maximum number of attempts.
            delay: Delay between attempts in seconds.
            timeout: Timeout per attempt.

        Returns:
            tuple[int, str, str]: (return_code, stdout, stderr)
        """
        for attempt in range(max_attempts):
            return_code, stdout, stderr = SystemUtils.run_command(
                command, timeout=timeout
            )

            if return_code == 0:
                return return_code, stdout, stderr

            if attempt < max_attempts - 1:
                time.sleep(delay)

        return return_code, stdout, stderr
