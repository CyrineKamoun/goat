# Custom SQL

The **Custom SQL** tool allows you to write custom SQL queries for data analysis directly within your workflows. This powerful feature enables advanced data processing that goes beyond GOAT's built-in tools.

:::warning Advanced Feature
This is an advanced feature intended for users with SQL knowledge. Incorrect queries may cause workflows to fail or produce unexpected results. If you need help writing SQL queries, you can use AI assistants to help generate and explain the code.
:::

## Overview

The Custom SQL tool connects to GOAT's DuckDB backend, giving you direct access to query your datasets using SQL syntax. You can:

- Execute complex analytical queries
- Join multiple datasets
- Perform aggregations and statistical calculations  
- Create derived datasets with custom logic
- Access advanced spatial functions

## Using Custom SQL

### Adding the Tool

<div class="step">
  <div class="step-number">1</div>
  <div class="content">From the <strong>Tools Panel</strong>, drag the <strong>Custom SQL</strong> tool onto your workflow canvas.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Connect input dataset nodes or other tools to provide data sources for your query.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Click on the Custom SQL node to open the <strong>Configuration Panel</strong> on the right.</div>
</div>

### Writing SQL Queries

In the configuration panel, you'll find a SQL editor where you can write your custom query:

```sql
SELECT 
  h.*,
  p.population_density,
  ST_Distance(h.geom, p.geom) AS distance_to_center
FROM input_1 h
JOIN input_2 p ON ST_Intersects(h.geom, p.geom)
WHERE p.population_density > 1000
ORDER BY distance_to_center
```

#### Input References

- **input_1, input_2, input_3...**: Reference your connected datasets using these table names
- The number corresponds to the connection order on the node
- You can connect up to 3 input datasets per Custom SQL node

#### Available Functions

The Custom SQL tool supports standard SQL functions plus spatial operations:

**Spatial Functions:**
- `ST_Distance()` - Calculate distances between geometries
- `ST_Intersects()` - Check if geometries intersect
- `ST_Within()` - Test if geometry is within another
- `ST_Buffer()` - Create buffers around geometries
- `ST_Area()` - Calculate geometry area
- `ST_Length()` - Calculate line length

**Analytical Functions:**
- `AVG()`, `SUM()`, `COUNT()` - Statistical aggregations
- `PERCENTILE_CONT()` - Calculate percentiles
- `ROW_NUMBER()`, `RANK()` - Window functions
- `CASE WHEN` - Conditional logic

### Query Validation

<div class="step">
  <div class="step-number">1</div>
  <div class="content"><strong>Syntax Check</strong>: The editor highlights syntax errors as you type.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content"><strong>Preview Results</strong>: Use the <code>Preview</code> button to see the first few rows of results.</div>
</div>

## Examples

### Basic Filtering and Selection
```sql
-- Select buildings within a certain area
SELECT building_type, height, geom
FROM input_1 
WHERE building_type = 'residential' 
  AND height > 10
```

### Spatial Join Analysis
```sql
-- Find all amenities within 500m of transit stops
SELECT 
  a.name as amenity_name,
  a.amenity_type,
  t.stop_name,
  ST_Distance(a.geom, t.geom) as distance
FROM input_1 a
JOIN input_2 t ON ST_DWithin(a.geom, t.geom, 500)
ORDER BY distance
```

### Aggregation by Area
```sql
-- Count points by administrative area
SELECT 
  admin.district_name,
  COUNT(points.*) as point_count,
  admin.geom
FROM input_1 points
RIGHT JOIN input_2 admin 
  ON ST_Within(points.geom, admin.geom)
GROUP BY admin.district_name, admin.geom
```

## Best Practices

:::tip Performance
- Use spatial indexes by including geometric predicates in WHERE clauses
- Limit results during development with `LIMIT 100`
- Test with small datasets first, then scale up
:::

:::warning Data Types
- Ensure geometry columns are properly formatted for spatial operations
- Cast data types explicitly when joining different datasets
- Check for NULL values in critical columns
:::

### Query Optimization

**Use Spatial Predicates**: Always include spatial filters like `ST_DWithin()` when possible to utilize spatial indexes.

**Column Selection**: Select only the columns you need rather than using `SELECT *`.

**Proper Joins**: Use appropriate join types (INNER, LEFT, RIGHT) based on your analysis needs.

### Error Handling

Common issues and solutions:

- **"Table not found"**: Ensure input datasets are properly connected
- **"Column doesn't exist"**: Check column names in your input datasets  
- **"Geometry error"**: Verify geometry columns are valid and properly formatted
- **"Timeout"**: Break complex queries into smaller steps or increase timeout

## Output and Integration

The Custom SQL tool creates a new temporary layer containing your query results. You can:

- Connect the output to other workflow tools for further analysis
- Add an export node to save results as a permanent dataset
- Use the results in visualizations and styling

:::info Variables Support
Custom SQL queries support [workflow variables](variables.md) using the `{{@variable_name}}` syntax for parameterized queries.
:::

## Limitations

- Maximum of 3 input datasets per Custom SQL node
- Queries must return at least one geometry column for mapping
- Some advanced DuckDB functions may not be available
- Query execution time is limited by the configured timeout

For more complex analysis requirements, consider using multiple Custom SQL nodes or combining with other workflow tools.
