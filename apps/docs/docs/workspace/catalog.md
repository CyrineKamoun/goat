---
sidebar_position: 4
---

# Catalog

The Data Catalog is your gateway to exploring Plan4Better's comprehensive collection of high-quality [geospatial datasets](../further_reading/glossary#geospatial-data). **This curated library provides reliable, ready-to-use data from official open-data providers and other trusted sources**, enabling you to immediately start analysis and visualization within your GOAT projects. From the Catalog you can:

- **Explore our dataset collection** spanning multiple thematic areas and geographic regions
- **Search and filter through it** by keyword, location, category, publisher, licence and more
- **Save the datasets you use often** and add them straight to a project

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/workspace/catalog/catalog_general.webp').default} alt="Data Catalog" style={{ maxHeight: "auto", maxWidth: "100%"}}/>
</div>

## Finding a dataset

Open the **Catalog** from the sidebar, or reach it from `+ Add layer` inside a project. The page lists every dataset in the catalog. Search, filters and sorting all narrow that list, and copying the page link shares the result — whoever opens it sees the same datasets you did.

### Search

Type into the search box to match a dataset's title, description and keywords.

Matching is on whole or partial words and ignores capitals, so `grünfläche` finds *Grünflächen*. It does not work the other way round: a longer word than the one in the data matches nothing, so prefer the shorter stem when a search comes back empty.

### Filters

The sidebar narrows the list by:

- **Data type** — feature, table, raster or bundle
- **Geometry** — point, line or polygon
- **Data Category** — the dataset's theme: environment, landuse, places, transportation, people, boundary, basemap or other
- **Publisher** — who distributes the dataset
- **Licence** — the terms it is published under
- **Language** — the language of its metadata
- **Period** — a `From` and `To` date, to find data covering a particular time

Each option shows how many datasets carry it, and the counts update as you narrow. Selected filters appear as chips above the results, so you can remove one without reopening the sidebar, and `Clear` removes them all.

### Filtering by location {#spatial-filter}

`Set spatial filter` narrows the catalog to datasets covering an area you care about. There are three ways to describe it:

- **Region** — search for a country, state or district by name
- **Point + buffer** — click the map to place a point, then set a radius around it
- **Polygon** — click the map to draw the corners of an area

Results are ranked by how much of each dataset falls inside your area, so the datasets that cover it best come first.

### Sorting and views

Sort by **Relevance**, **Last updated**, or title **A–Z** / **Z–A**, and switch between a list and a grid of cards. Relevance takes your reading language into account, so a German interface surfaces German datasets first.

### Saving datasets you use often

The star on a dataset card saves it. `Show my favourites` then narrows the catalog to what you have saved. Saved datasets persist across sessions and are shared with the `+ Add layer` picker inside projects.

## What a dataset page shows

Click a dataset to open it. The **Summary** tab shows:

- **Description** of the dataset's content and scope
- **Type**, **Geometry type**, **Data Category**, **Language** and **Region**
- **License**, with `View license at source` where the publisher links to the terms
- **Data Reference Year** and when the dataset was **Last updated**
- **Key words** the publisher tagged it with
- **Publisher**, with contact details where they are available

Where a dataset has data to show, a **Data** tab holds a **Data sample** of its rows and a **Columns** list naming every column and its type. The sample is capped, so it is a look at the data rather than the whole of it.

A dataset made of several layers is marked `Bundle` and lists **Layers in this bundle**; opening one shows which bundle it belongs to. See [Dataset Types](../data/dataset_types.md#datasets-made-of-several-layers) for what that means.

## Adding catalog data to a project

Datasets are added to a project from inside that project, so the `+ Add layer` dialog is where this happens. Browsing the Catalog page on its own is for finding and saving datasets; its `Add to project` button is not active yet.


<div class="step">
  <div class="step-number">1</div>
  <div class="content">In your project, open the <strong>Layers</strong> tab and click <code>+ Add layer</code>.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Choose the <code>Catalog</code> tab to browse the same datasets, with the same search and filters.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Select one or more datasets and click <code>Add to project</code>.</div>
</div>

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/workspace/catalog/catalog_add-layer.gif').default} alt="Adding a catalog dataset to a project" style={{ maxHeight: "700px", maxWidth: "800px"}}/>
</div>

<p></p>

The first time a catalog dataset is added, GOAT prepares a copy of it for your project. The layer shows `Preparing data …` while that runs and becomes available when it finishes.

:::info Catalog layers are read-only
A layer added from the catalog is marked `Catalog · read-only`. You can style, filter and analyse it like any other layer, but its rows and columns cannot be edited, and it cannot be downloaded directly — the licence and the source stay with the publisher.
:::

When a newer version of a dataset is published, a layer already in your project shows `Update available in the catalog`, so you can decide whether to bring in the new data.

:::tip Hint
After adding the layer, you can apply [Filters](../map/filter.md "Filter dataset") to constrain large datasets to specific geographic extents or attributes needed for your analysis
:::

## What the catalog holds

The catalog covers several thematic areas. These are some of the main ones.

### Points of Interest (POIs)
**Strategic locations of amenities, facilities, and attractions essential for accessibility planning and urban analysis**, such as Public transport stops and stations, Shopping centers and retail locations, Tourism and leisure facilities, Food and beverage establishments, Healthcare facilities and hospitals, Educational institutions and schools.

*Data Sources:* [Overture Maps Foundation](https://overturemaps.org/), [OpenStreetMap (OSM)](https://wiki.openstreetmap.org/), government departments, health insurance providers, and retail companies. Additional field data collection conducted when necessary.

### Population and Buildings
**Detailed demographic data disaggregated to building and local levels**, enhanced with land-use information for improved accuracy. We provide Building-level population data for German districts and municipalities, local population data from German Census 2022, and European NUTS-3 level population statistics (Nomenclature of Territorial Units for Statistics).

*Data Sources:* [German Census 2022](https://ergebnisse.zensus2022.de/datenbank/online/), individual municipalities and districts, and 3D City Models from German federal states.

### Administrative Boundaries
**Comprehensive boundary datasets defining governmental and administrative jurisdictions at multiple scales**, such as Municipal boundaries, District boundaries, Federal state boundaries, and Postal code regions.

*Data Sources:* [Federal Agency for Cartography and Geodesy (BKG)](https://www.bkg.bund.de/) and [OpenStreetMap (OSM)](https://wiki.openstreetmap.org/).

## Data quality and maintenance

Plan4Better ensures the reliability and currency of catalog data through comprehensive data management processes:

### Data collection and preparation

Our data collection process follows rigorous standards to ensure quality and reliability:

- **Source identification** - We prioritize official open data portals and publicly available initiatives
- **Format standardization** - Various formats (shapefiles, GeoJSON, etc.) are converted to consistent schemas
- **Data integration** - Multiple datasets are combined and adapted to local contexts through fusion workflows
- **Quality validation** - Comprehensive validation processes ensure accuracy and reliability
- **Continuous expansion** - We actively seek and integrate additional datasets based on user needs

### Update schedule

To maintain data currency and relevance:

- **Annual updates** - All datasets are refreshed at least once per year
- **Dynamic data** - Rapidly changing data (POIs, public transport) receives more frequent updates
- **On-demand updates** - Critical datasets can be updated as needed based on user requirements

