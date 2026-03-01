import logging
import sys

# Setup basic logging (In prod, this would use the libs.utils.log module)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

from src.triggers.cli import CommandLineTrigger
from src.triggers.file import FileTrigger
from src.triggers.resume import ResumeTrigger
from src.triggers.resume import InternalTrigger
from src.triggers.scheduler import SchedulerTrigger
from src.utils.cli import setup_parser


def main():
    """Main entry point for the Ingestion application."""
    parser = setup_parser()
    args = parser.parse_args()

    # The file watcher needs to know the path to this script to spawn new processes.
    args.script_path = sys.argv[0]

    trigger_map = {
        "ingest": CommandLineTrigger,
        "watch": FileTrigger,
        "resume": ResumeTrigger,
        "resume": InternalTrigger,
        "schedule": SchedulerTrigger,
    }

    if trigger_class := trigger_map.get(args.command):
        trigger = trigger_class(args)
        trigger.run()
    else:
        logger.error(f"Unknown command: {args.command}")
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
