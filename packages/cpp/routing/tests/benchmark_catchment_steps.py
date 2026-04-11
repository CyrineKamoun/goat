from __future__ import annotations

import argparse
import json
import statistics

import routing


def _derive_metrics(run: routing.CatchmentStepBenchmark) -> dict[str, float]:
    # Keep read/route/output tied to explicit C++ phase timings.
    # Build pre-process explicitly from preprocessing subtasks.
    read_ms = float(run.network_read_ms)
    validation_ms = float(run.validation_ms)
    class_selection_ms = float(run.class_selection_ms)
    buffer_distance_ms = float(run.buffer_distance_ms)
    compute_costs_ms = float(run.compute_costs_ms)
    build_network_ms = float(run.build_network_ms)
    snap_ms = float(run.snap_ms)

    preprocess_ms = (
        validation_ms
        + class_selection_ms
        + buffer_distance_ms
        + compute_costs_ms
        + build_network_ms
        + snap_ms
    )
    route_ms = float(run.routing_ms)
    output_ms = float(run.conversion_ms)
    total_ms = float(run.result_ms)

    return {
        "read_ms": read_ms,
        "pre_process_ms": preprocess_ms,
        "route_ms": route_ms,
        "output_ms": output_ms,
        "total_ms": total_ms,
        "detail_validation_ms": validation_ms,
        "detail_class_selection_ms": class_selection_ms,
        "detail_buffer_distance_ms": buffer_distance_ms,
        "detail_compute_costs_ms": compute_costs_ms,
        "detail_build_network_ms": build_network_ms,
        "detail_snap_ms": snap_ms,
    }


def build_config(args: argparse.Namespace) -> routing.RequestConfig:
    cfg = routing.RequestConfig()
    cfg.starting_points = [routing.Point3857(args.start_x, args.start_y)]
    cfg.mode = getattr(routing.RoutingMode, args.mode)
    cfg.cost_mode = getattr(routing.CostMode, args.cost_mode)
    cfg.max_traveltime = args.max_traveltime
    cfg.steps = args.steps
    cfg.speed_km_h = args.speed_km_h
    cfg.edge_dir = args.edge_dir
    cfg.catchment_type = getattr(routing.CatchmentType, args.catchment_type)
    cfg.output_format = routing.OutputFormat.GeoJSON
    cfg.polygon_difference = args.polygon_difference
    return cfg


def summarize_run(run: routing.CatchmentStepBenchmark) -> dict[str, float]:
    metrics = _derive_metrics(run)
    return {name: round(value, 2) for name, value in metrics.items()}


def main() -> None:
    parser = argparse.ArgumentParser(description="Canonical catchment benchmark runner")
    parser.add_argument("--edge-dir", required=True)
    parser.add_argument("--start-x", type=float, default=1288578.0)
    parser.add_argument("--start-y", type=float, default=6130064.0)
    parser.add_argument("--mode", choices=["Walking", "Bicycle", "Pedelec", "Car"], default="Car")
    parser.add_argument("--cost-mode", choices=["Time", "Distance"], default="Time")
    parser.add_argument("--max-traveltime", type=float, default=60.0)
    parser.add_argument("--steps", type=int, default=10)
    parser.add_argument("--speed-km-h", type=float, default=30.0)
    parser.add_argument("--catchment-type", choices=["Polygon", "Network", "HexagonalGrid"], default="Network")
    parser.add_argument("--polygon-difference", action="store_true")
    parser.add_argument("--warmup-runs", type=int, default=1)
    parser.add_argument("--runs", type=int, default=5)
    args = parser.parse_args()

    cfg = build_config(args)

    warmups = [routing.benchmark_catchment_steps(cfg) for _ in range(max(0, args.warmup_runs))]
    runs = [routing.benchmark_catchment_steps(cfg) for _ in range(max(1, args.runs))]

    derived_runs = [_derive_metrics(r) for r in runs]

    metric_names = [
        "read_ms",
        "pre_process_ms",
        "route_ms",
        "output_ms",
        "total_ms",
    ]

    detail_metric_names = [
        "detail_validation_ms",
        "detail_class_selection_ms",
        "detail_buffer_distance_ms",
        "detail_compute_costs_ms",
        "detail_build_network_ms",
        "detail_snap_ms",
    ]

    output = {
        "config": {
            "edge_dir": args.edge_dir,
            "start": [args.start_x, args.start_y],
            "mode": args.mode,
            "cost_mode": args.cost_mode,
            "max_traveltime": args.max_traveltime,
            "steps": args.steps,
            "speed_km_h": args.speed_km_h,
            "catchment_type": args.catchment_type,
            "polygon_difference": args.polygon_difference,
            "warmup_runs": args.warmup_runs,
            "runs": args.runs,
        },
        "warmup": [summarize_run(r) for r in warmups],
        "runs": {
            name: [round(m[name], 2) for m in derived_runs]
            for name in metric_names
        },
        "median": {
            name: round(statistics.median(m[name] for m in derived_runs), 2)
            for name in metric_names
        },
        "detail_median": {
            name: round(statistics.median(m[name] for m in derived_runs), 2)
            for name in detail_metric_names
        },
        "edge_count": runs[0].edge_count,
        "node_count": runs[0].node_count,
        "payload_bytes": runs[0].payload_bytes,
    }
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
