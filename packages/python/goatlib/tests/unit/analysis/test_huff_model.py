from pathlib import Path

import pytest
from goatlib.analysis.accessibility.huff_model import HuffmodelTool
from goatlib.analysis.schemas.heatmap import (
    HuffmodelParams,
)
from pydantic import ValidationError


def test_huffmodel_schema_validation() -> None:
    """Test that HuffmodelParams schema validates correctly"""

    # Valid minimal parameters
    valid_params = {
        "routing_mode": "walking",
        "od_matrix_path": "/path/to/matrix.parquet",
        "output_path": "/path/to/output.parquet",
        "reference_area_path": "/path/to/reference.geojson",
        "demand_path": "/path/to/demand.geojson",
        "demand_field": "population",
        "opportunity_path": "/path/to/opportunities.geojson",
        "attractivity": "capacity",
        "max_cost": 20,
        "attractiveness_param": 1.0,
        "distance_decay": 1.0,
    }

    # Should create successfully
    params = HuffmodelParams(**valid_params)
    assert params.attractiveness_param == 1.0
    assert params.distance_decay == 1.0
    assert params.routing_mode == "walking"
    assert params.attractivity == "capacity"

    # Test invalid attractiveness_param (must be positive)
    invalid_params = valid_params.copy()
    invalid_params["attractiveness_param"] = -1.0
    with pytest.raises(ValidationError):
        HuffmodelParams(**invalid_params)

    # Test invalid distance_decay (must be positive)
    invalid_params = valid_params.copy()
    invalid_params["distance_decay"] = 0.0
    with pytest.raises(ValidationError):
        HuffmodelParams(**invalid_params)

    # Test missing required field
    invalid_params = valid_params.copy()
    del invalid_params["opportunity_path"]
    with pytest.raises(ValidationError):
        HuffmodelParams(**invalid_params)

    print("Schema validation tests passed")


def test_huff_model_workflow(data_root: Path, walking_matrix_dir: Path) -> None:
    """Test the Huff model workflow using kindergarten data"""
    result_dir = Path(__file__).parent.parent.parent / "result"
    result_dir.mkdir(parents=True, exist_ok=True)

    # Opportunity layer: kindergartens in eastern Munich, with their capacity
    opportunity_path = str(data_root / "analysis" / "kita_part_munich.gpkg")
    attractivity = "capacity_int"
    max_cost = 30
    # Create demand layer using census data (population needing kindergarten services)
    demand_path = str(data_root / "analysis" / "census_part_munich.parquet")
    demand_field = "einwohner"  # Population count as demand

    reference_area_path = str(data_root / "analysis" / "munich_districts.geojson")
    params = HuffmodelParams(
        routing_mode="walking",
        od_matrix_path=str(walking_matrix_dir),
        output_path=str(result_dir / "unit_huff_model_kindergarten.parquet"),
        reference_area_path=str(reference_area_path),
        attractiveness_param=5.0,
        demand_path=demand_path,
        demand_field=demand_field,
        opportunity_path=opportunity_path,
        attractivity=attractivity,
        max_cost=max_cost,
    )

    tool = HuffmodelTool()
    results = tool.run(params)

    # Assertions to make it a proper test
    assert results is not None
    assert len(results) == 1
    assert Path(results[0][1].path).exists()


def test_huff_computation_with_known_values() -> None:
    """Test Huff model computation with known input values to verify correctness"""
    tool = HuffmodelTool()

    # Create simple test scenario with known values
    # OD matrix: 2 origins, 2 destinations
    tool.con.execute("""
        CREATE TABLE test_od_matrix AS
        SELECT 'orig_1' AS orig_id, 'supply_1' AS dest_id, 10.0 AS cost
        UNION ALL SELECT 'orig_1', 'supply_2', 20.0
        UNION ALL SELECT 'orig_2', 'supply_1', 15.0
        UNION ALL SELECT 'orig_2', 'supply_2', 5.0
    """)

    # Supply table with known attractiveness
    tool.con.execute("""
        CREATE TABLE test_supply AS
        SELECT 'supply_1' AS supply_id, 'supply_1' AS dest_id, 100 AS attractivity, 25 AS max_cost
        UNION ALL SELECT 'supply_2', 'supply_2', 200, 25
    """)

    # Demand table
    tool.con.execute("""
        CREATE TABLE test_demand AS
        SELECT 'orig_1' AS orig_id, 300.0 AS demand_value
        UNION ALL SELECT 'orig_2', 400.0
    """)

    # Reference table (dummy)
    tool.con.execute("""
        CREATE TABLE test_reference AS
        SELECT 'study_area' AS study_area_id
    """)

    # Compute Huff model with known parameters
    result_table = tool._compute_huff_model(
        "test_od_matrix",
        "test_supply",
        "test_demand",
        attractiveness_param=1.0,  # Linear attractiveness
        distance_decay=1.0,  # Linear distance decay
        max_cost=25,
    )

    # Verify results
    results = tool.con.execute(
        f"SELECT * FROM {result_table} ORDER BY supply_id"
    ).fetchdf()

    assert len(results) == 2, "Should have results for both supply locations"
    # `probability` is the share of total demand captured, in percent (0-100)
    assert all(
        prob >= 0 for prob in results["probability"]
    ), "Probabilities should be non-negative"
    assert all(
        prob <= 100 for prob in results["probability"]
    ), "Probabilities should be <= 100"

    # Calculate expected captured demand manually
    # For origin 'orig_1' (demand 300):
    # - supply_1: utility = 100^1.0 * 10^-1.0 = 10
    # - supply_2: utility = 200^1.0 * 20^-1.0 = 10
    # - P(supply_1) = P(supply_2) = 0.5 -> captured 150 each

    # For origin 'orig_2' (demand 400):
    # - supply_1: utility = 100^1.0 * 15^-1.0 = 6.667
    # - supply_2: utility = 200^1.0 * 5^-1.0 = 40
    # - P(supply_1) = 0.143, P(supply_2) = 0.857 -> captured 57.14 / 342.86

    # Total demand 700:
    # - supply_1: (150 + 57.14) / 700 * 100 = 29.59 %
    # - supply_2: (150 + 342.86) / 700 * 100 = 70.41 %
    supply1_result = results[results["supply_id"] == "supply_1"].iloc[0]
    supply2_result = results[results["supply_id"] == "supply_2"].iloc[0]

    assert supply1_result["probability"] == pytest.approx(29.59, abs=0.1)
    assert supply2_result["probability"] == pytest.approx(70.41, abs=0.1)

    # All demand is within max_cost, so shares should sum to 100 %
    total_prob = results["probability"].sum()
    assert total_prob == pytest.approx(
        100.0, abs=0.1
    ), f"Total probability {total_prob} should sum to 100 %"
    print(
        f"Huff model results: {len(results)} facilities, total probability: {total_prob:.3f}"
    )
    print(f"Supply 1 probability: {supply1_result['probability']:.3f}")
    print(f"Supply 2 probability: {supply2_result['probability']:.3f}")

    # Cleanup
    for table in [
        "test_od_matrix",
        "test_supply",
        "test_demand",
        "test_reference",
        result_table,
    ]:
        tool.con.execute(f"DROP TABLE IF EXISTS {table}")
