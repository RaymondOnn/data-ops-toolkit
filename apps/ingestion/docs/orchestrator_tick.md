``` mermaid
graph TD
    Tick[Orchestrator Tick] --> Sensor[Signal Processor: Scan signals/]
    Sensor -->|Found .done| Janitor[Janitor: Clean Workspace]
    Sensor -->|Found .fail| Quarantine[Move to FAILED/ folder]
    Sensor -->|Found .sync| Store[StateStore: Sync to metadata database]

    Tick --> Trigger[TriggerManager: Check Cron/Files]
    Trigger -->|Ready| Dispatch[TaskManager: Check Resources]
    Dispatch -->|Fits| Ray[Spawn Ray Worker]
    Dispatch -->|Full| Wait[Queue as WAITING]
```
