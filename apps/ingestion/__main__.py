from libs.utils.log import setup_logging
import sys

# Setup basic logging (In prod, this would use the libs.utils.log module)
# Call this ONCE before any work starts
setup_logging(log_dir="logs", is_prod=False)


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
