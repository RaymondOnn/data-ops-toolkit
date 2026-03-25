"""
apps/ingestion/tests/check_circular_imports.py
Run this from the apps/ingestion directory:
python -m tests.check_circular_imports
"""

import sys
from pathlib import Path

# Add the src root to python path so we can simulate how the app runs
current_dir = Path(__file__).parent.parent
sys.path.append(str(current_dir))


def check_import(module_name: str) -> None:
    print(f"🔄 Attempting to import: {module_name}...", end=" ")
    try:
        __import__(module_name)
        print("✅ Success")
    except Exception as e:
        print("\n❌ FAILED")
        print(f"   Error: {e}")
        # Identify typical circular import message
        if "partially initialized" in str(e) or "cannot import name" in str(e):
            print("   👉 This strongly suggests a circular import detected!")
        sys.exit(1)


if __name__ == "__main__":
    print("--- 🕵️‍♀️ Circular Import Detector ---")

    # 1. Check Base Definitions (Lowest Level)
    check_import("src.core.models.steps.base")

    # 2. Check Individual Steps (Mid Level)
    check_import("src.core.models.steps.extract")
    check_import("src.core.models.steps.transform")

    # 3. Check Package Init (Aggregation Level)
    check_import("src.core.models.steps")

    # 4. Check Orchestrator (High Level)
    check_import("src.core.orchestrator.orchestrator")

    print("\n🎉 No immediate circular imports detected in core paths.")
