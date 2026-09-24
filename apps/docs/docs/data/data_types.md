---
sidebar_position: 2
sidebar_label: Field Types
---

# Field Types

Every column in a feature dataset or table has a **field type**, which tells GOAT how to store the values, how to display them, and which tools can use the column. This page explains the field types GOAT supports.

## Spatial Data Storage

GOAT keeps a dataset's **metadata** separate from the **data** itself:

- **Metadata**: the dataset's name, description, field definitions, styling and sharing settings. This is held in a **PostgreSQL** database.
- **Data**: the geometries and attribute values. These are stored in **DuckLake**, an analytical store built on Parquet files in object storage. This is what lets GOAT query and analyse large datasets quickly.

All geometries are stored in the **EPSG:4326** coordinate reference system (longitude and latitude in degrees). Measurements are not taken in degrees, though: **Area**, **Perimeter** and **Length** columns are calculated on the earth's curved surface, so their values are true meters and stay accurate across large areas.

For a fuller picture of how the platform fits together, see [Architecture](../nerdy_content/architecture.md).

## Supported Field Types

| Field type | What it holds | Examples |
|------------|---------------|----------|
| **Text** | Any text | Street names, categories, descriptions |
| **Number** | Whole numbers or decimals | `1`, `-5`, `3.14`, `45.67` |
| **Boolean** | True or false values | `true`, `false` |
| **Date** | A point in time | `2024-01-15 14:30:00` |
| **Area** | The surface area of a polygon | `1.25 ha`, `340 m²` |
| **Perimeter** | The outline length of a polygon | `820 m` |
| **Length** | The length of a line | `1.4 km` |
| **Formula** | A value calculated from other columns | `population / area` |

### Columns you fill in yourself

**Text**, **Number**, **Boolean** and **Date** hold the values you enter or import. You can add them to a dataset at any time, and they are the types available when you create a new dataset from scratch.

A **Number** column can be formatted for display. Formatting changes only how the value looks in GOAT. The stored value is untouched, and tools keep using the full number.

| Setting | Effect on `1234567.891` |
|---------|--------------------------|
| **Decimals** | `2` rounds the display to `1234567.89`; `auto` lets GOAT decide |
| **Show thousands separator** | `1,234,567.891` |
| **Abbreviate large numbers** | `1.2M` |
| **Always show sign** | `+1234567.891`, so positive values are as clearly marked as negative ones |

The same settings are available on **Area**, **Perimeter** and **Length** columns, which additionally let you pick the **Unit** the value is shown in: `m²`, `ha` or `km²` for Area, and `m` or `km` for Perimeter and Length. Leave it on `auto` and GOAT picks a unit that suits the value.

GOAT shows a live preview as you change these, so you can see the result before saving.

### Columns GOAT calculates

**Area**, **Perimeter**, **Length** and **Formula** are **computed**: GOAT derives their values and keeps them up to date, so they cannot be edited by hand. They are offered when you edit the fields of an existing dataset. A new dataset has no data for them to be calculated from yet.

**Formula** can be added to any dataset. The measurement types depend on your dataset's geometry, since not every shape can be measured the same way:

- **Polygons**: Area and Perimeter
- **Lines**: Length
- **Points**: neither, as a point has no extent
- **Tables** (no geometry): neither

A **Formula** column is defined by an expression you write in the Formula builder, and GOAT recalculates it whenever the data it depends on changes.

## Restricting what a column accepts

**Text** and **Number** columns can carry a list of **Allowed values**. Everyone editing the dataset then picks from that list instead of typing freely, which keeps categories consistent. No more `bus stop`, `Bus Stop` and `bus_stop` in the same column.

<div class="step">
  <div class="step-number">1</div>
  <div class="content">In the data table, open the <strong>Text</strong> or <strong>Number</strong> column you want to restrict: either click <code>Edit fields</code> and select it from the list, or click the <img src={require('/img/icons/3dots.png').default} alt="Options" style={{ maxHeight: "16px", maxWidth: "16px", verticalAlign: "middle"}}/> menu on its column header and choose <code>Edit field</code>.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Under <code>Allowed values</code>, type a value and press <code>Enter</code>. Repeat for each value you want to allow.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Once the list has at least one value, an <code>Allow other values</code> switch appears. Turn it on to treat the list as a suggestion rather than a rule.</div>
</div>

<div class="step">
  <div class="step-number">4</div>
  <div class="content">Click <code>Save</code>.</div>
</div>

Leaving the list empty accepts any value, which is how every column starts.

:::info Reserved column names
Some names are used internally by GOAT and cannot be given to a column, among them `id`, `geometry` and `geom`. If you type one, GOAT flags the field straight away and will not let you save until you change it.
:::

## How to view field types

GOAT marks each column with a small **field type chip**: `A` for Text, `123` for Number, and an icon for the other types. Hover over the chip to see the field type's name.

The chip appears wherever columns are listed: the data table, the field list in layer settings, column filters, and the field pickers in tools and the Formula builder.

To see it in the data table:

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Click on <img src={require('/img/icons/3dots.png').default} alt="Options" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/> <code>More Options</code> button on your layer</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Select <code>View data</code> from the menu</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">In the data table, the field type chip is shown beside each column name</div>
</div>

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/data/view_data.webp').default} alt="More Options" style={{ maxHeight: "auto", maxWidth: "auto", objectFit: "cover"}}/>
  <p style={{ textAlign: 'center', fontStyle: 'italic', marginTop: '8px', color: '#666' }}>Accessing the View Data option</p>
</div>



<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/data/data-table.webp').default} alt="Data table showing field type chips" style={{ maxHeight: "auto", maxWidth: "auto", objectFit: "cover"}}/>
  <p style={{ textAlign: 'center', fontStyle: 'italic', marginTop: '8px', color: '#666' }}>Field type chips shown beside each column name</p>
</div>

