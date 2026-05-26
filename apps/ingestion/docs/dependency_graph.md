# Dependency Graph

This document illustrates the structural dependencies between modules and the logical execution flow of the ingestion pipeline.

## 1. Module-Level Hierarchy
This graph shows how the `ingestion` app consumes the shared `libs/` layer and how internal components are coupled.

```mermaid
graph TD
    subgraph APPS ["apps/ingestion (The Orchestrator)"]
        CLI["CLI (Typer/Actions)"]
        RT["Runtime (Daemon/Trigger)"]
        ORCH["Core Orchestrator"]
        STG["Execution Stages"]
        STRAT["Strategies (Extract/Transform/Load)"]
    end

    subgraph LIBS ["libs/ (Shared Core)"]
        DB["libs/database"]
        FILE["libs/file"]
        RES["libs/resilience"]
        CACHE["libs/cache"]
        AUTH["libs/auth"]
        UTILS["libs/utils"]
    end

    %% App Internal Dependencies
    CLI --> RT
    RT --> ORCH
    ORCH --> STG
    STG --> STRAT

    %% App to Lib Dependencies
    ORCH --> CACHE
    ORCH --> RES
    
    STG --> FILE
    STG --> DB
    
    STRAT --> UTILS
    STRAT --> FILE
    
    %% Lib to Lib Dependencies
    DB --> AUTH
    DB --> RES
    FILE --> AUTH
    FILE --> UTILS
    RES --> CACHE
```

## 2. Execution Stage Dependencies (The Pipeline)
This graph represents the state-machine transitions and physical data dependencies. A failure in an upstream stage typically triggers a `RewindTask` or quarantine logic.

```mermaid
graph LR
    START((Start)) --> EXTRACT[Extract Stage]
    
    subgraph COMPUTE ["Ray Distributed Work"]
        EXTRACT --> TRANSFORM[Transform Stage]
        TRANSFORM --> WRITE[Write Stage]
    end

    WRITE --> PUBLISH[Publish Stage]
    PUBLISH --> ARCHIVE[Archive Stage]
    ARCHIVE --> END((Finish))

    %% Data Dependencies
    EXTRACT -.->|Source Artifacts| TRANSFORM
    TRANSFORM -.->|Transformed Parquet| WRITE
    WRITE -.->|Staging Table| PUBLISH

    %% Self-Healing Loops
    TRANSFORM -- Missing Data --> EXTRACT
    WRITE -- Missing Data --> TRANSFORM
    PUBLISH -- Count Mismatch --> WRITE
```

## Summary of Coupling

- **Loose Coupling (Services)**: The stages depend on `ServiceFactory`, not concrete implementations like `ClickHouseService`. This allows the engine to support new databases without modifying the orchestrator core.
- **Strong Coupling (State)**: All components are strongly coupled to the `ExecutionContext` and `TaskManifest`. This ensures a single source of truth for the "Physical State" on disk.