# Catchment + Nigiri Architecture Diagram

```mermaid
flowchart TD
    REQ[Request - incoming API or SDK call]

    subgraph IN[Input Layer]
        V[Validation - check and normalize parameters]
        CFG[Request Config - canonical routing configuration]
    end

    subgraph DATA[Data Access Layer]
        EL[Parquet Edge Loader - read road network from DuckDB]
        TL[Transit Timetable Loader - load GTFS timetables via Nigiri]
    end

    subgraph KERNEL[Routing Kernel]
        SEL{Mode Selector - pick transport mode adapter}
        MEL[Edge Loader - fetch edges bounded by mode and budget]

        subgraph ROAD[Road and Active Mobility]
            WA[Walking Dijkstra - shortest path on walk network]
            BA[Cycling Dijkstra - shortest path on bike network]
            CA[Car Dijkstra - shortest path on road network]
        end

        RF[Reachability Field - location or cell id with travel cost]
    end

    subgraph PT[Public Transport Pipeline]
        ACC[Access - catchments from origin]
        SEED[Seed Stops - intersect stops with access catchment]
        M2A[Nigiri PT Routing - one-to-all transit from seed stops]
        DEST[Destination Stops - collect reached stops and transit times]
        EGR[Egress - catchments from destination stops]
        CSR[Combine Catchments - merge into unified cost surface]
    end

    subgraph GEO[Geometry Pipeline]
        GT{Output Type - select geometry format}
        ISO[Isochrone Builder - contour polygons from cost surface]
        NET[Network Builder - extract reached network edges]
        H3[H3 Grid Adapter - snap results to hex grid]
        CG[Custom Grid Adapter - snap results to custom grid]
        BAND[JSOlines Processor - cut isochrone bands by time step]
    end

    subgraph OUT[Output]
        SER[GeoJSON Serializer - attach properties and CRS]
        RES[Response - final API response]
    end

    REQ --> V --> CFG

    CFG --> SEL

    SEL --> MEL
    MEL --> EL
    MEL --> TL

    MEL --> WA
    MEL --> BA
    MEL --> CA

    WA --> ACC
    ACC --> SEED --> M2A --> DEST --> EGR --> CSR --> RF
    WA --> EGR
    WA --> RF
    BA --> RF
    CA --> RF

    RF --> GT
    GT --> ISO
    GT --> NET
    GT --> H3
    GT --> CG

    ISO --> BAND --> SER
    NET --> SER
    H3 --> SER
    CG --> SER

    SER --> RES
```

## Notes
- Parquet edge loading is mode-aware and happens after mode selection.
- PT flow is explicit: access catchments -> seed stops -> nigiri one-to-all -> destination catchments -> cost surface and reachability.
- Final output adapters are polygon, network, H3 hex grid, and custom snapped grid.
