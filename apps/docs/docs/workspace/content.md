---
sidebar_position: 3
---

# Content

The **Content** page is where your projects, datasets and templates live. Everything you can reach is organised into **spaces** — your own, your teams', and your organisation's — so content belongs to a space rather than to you personally, and stays where it is when people join or leave.

From the Content page you can:

- **Browse everything in a space**, in folders you organise yourself
- **See what other people have shared with you**, and what you opened recently
- **Share, move, rename, transfer or delete** the content you are responsible for
- **Restore** anything you deleted by mistake

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/workspace/content/content_general.webp').default} alt="The Content page in GOAT" style={{ maxHeight: "auto", maxWidth: "100%"}}/>
</div>

## Spaces

The panel on the left lists the spaces you can reach:

- **My Content** — your personal space. Content you create lands here unless you put it somewhere else.
- **Team spaces** — one per team you belong to. Everyone in the team can reach what the space holds.
- **Organization** — shared across your whole organisation.

Below the spaces are three views that cut across all of them:

- **Shared with me** — everything other people have shared with you, gathered in one place
- **Recent** — what you have opened lately, across every space you can reach
- **Trash** — content you deleted, until it is purged

Selecting a space shows what it holds. Within a space, **Folders** group content however suits you.

## Finding your way around

The toolbar above the content offers:

- **Search** within the space you are in
- **Grid** or **List** view
- **Filter** by content type
- **Sort**, by `Last updated` and other options
- **Details**, which opens a panel describing the selected item
- **Add new**, to create a project, upload a dataset or add a folder

## Managing content

Select an item — or several — to act on them. The kebab menu on a card and the action bar offer:

| Action | What it does |
|--------|--------------|
| **Share** | Gives other people access. You stay the owner. |
| **Move** | Puts the item in a different folder. |
| **Rename** | Changes its name. |
| **Transfer Ownership** | Hands the item to someone else, permanently. |
| **Delete** | Sends it to the Trash. |

### Sharing and transferring are different

**Sharing** grants access while you remain the owner — useful when colleagues need to see or edit something that is still yours to look after.

**Transferring ownership** moves the item into someone else's space for good. Reach for it when a project genuinely changes hands, for example when you hand a piece of work over before leaving a team.

:::info Who can see what
An item shows its **audience** — private, shared with named people, shared with a team or organisation, or public. Folders and bundles can be shared with teams and the organisation.
:::

### Trash and restore

Deleting content does not remove it straight away: it goes to the **Trash**, where the owner can **Restore** it. Content stays there until it is purged, so a delete made in error is recoverable.

## Adding content

Use `Add new` on the Content page to create a project, upload a dataset or add a folder. A project can also be started from the Home page.

### Creating a project

Follow these simple steps to create a new project:

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Open the <code>Content</code> page from the sidebar.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Click <code>Add new</code> and choose a project option.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">
  <p>Fill in the required information:</p>
    <ul>
      <li><strong>Project Title</strong> - Give your project a descriptive name</li>
      <li><strong>Project Folder</strong> - Choose or create a folder to organize your project</li>
      <li><strong>Description</strong> (optional) - Add details about your project's purpose</li>
    </ul>
  </div>
</div>

<div class="step">
  <div class="step-number">4</div>
  <div class="content">Review your information and click the <code>Create</code> button to finalize your new project.</div>
</div>

### Importing a project

You can import a previously exported GOAT project file:

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Click <code>Add new</code> and choose a project option.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Select <code>Import</code> to upload an existing project file.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Choose a <strong>Project Folder</strong> and click <code>Import</code> to finish.</div>
</div>

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/workspace/projects/project_import.webp').default} alt="Import a project in GOAT" style={{ maxHeight: "auto", maxWidth: "100%", objectFit: "cover"}}/>
</div>

### Uploading a dataset

GOAT supports multiple file formats for upload: **GeoPackage**, **GeoJSON**, **Shapefile**, **KML**, **CSV**, **XLSX**, **ZIP**, **Parquet**, and **COG** files, as well as **GTFS** (`gtfs.zip`) archives for [Public Transport Networks](../data/dataset_types.md#public-transport-networks) and **Overture** (`overture.zip`) archives for [Street Networks](../data/dataset_types.md#street-networks).

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Open the <code>Content</code> page from the sidebar.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Click <code>Add new</code> and select <code>Dataset</code>.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">In the <strong>Select File</strong> step, choose the file from your local device. Supported formats are listed at the bottom of the dialog. Click <code>Next</code>.</div>
</div>

<div class="step">
  <div class="step-number">4</div>
  <div class="content"><strong>CSV and XLSX files only — Preview &amp; Configure step:</strong> GOAT shows a preview of your tabular data so you can verify it before import.
    <ul>
      <li><code>Worksheet</code> — For XLSX files with multiple sheets, select which sheet to import.</li>
      <li><code>First row is header</code> — Toggle on (default) if your file's first row contains column names. Toggle off if the first row is data — column names will be auto-generated and you can rename them later in the layer settings.</li>
    </ul>
    The preview table shows the first rows of your file. Click <code>Next</code> when ready.
  </div>
</div>

<div class="step">
  <div class="step-number">5</div>
  <div class="content">In the <strong>Destination &amp; Metadata</strong> step, configure your dataset:
    <ul>
      <li><strong>Destination Folder</strong> — Choose where to organize your dataset</li>
      <li><strong>Name</strong> — Give your dataset a descriptive name</li>
      <li><strong>Description</strong> (optional) — Add details about your dataset's content and purpose</li>
    </ul>
    Click <code>Next</code>.
  </div>
</div>

<div class="step">
  <div class="step-number">6</div>
  <div class="content">Review your configuration in the <strong>Confirmation</strong> step and click <code>Upload</code> to add the dataset to your workspace.</div>
</div>

### Connecting to an external source

Connect to external data services including **Web Feature Service (WFS)**, **Web Map Service (WMS)**, **Web Map Tile Service (WMTS)**, **XYZ Tiles**, and **Cloud Optimized GeoTIFF (COG)**.

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Open the <code>Content</code> page from the sidebar.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Click <code>Add new</code> and select <code>Dataset</code>, then choose the external-source option.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Enter the URL of the external data service.</div>
</div>

<div class="step">
  <div class="step-number">4</div>
  <div class="content">Select the specific layer you want to add from the available options and click <code>Next</code>.</div>
</div>

<div class="step">
  <div class="step-number">5</div>
  <div class="content">
  <p>Configure your dataset:</p>
    <ul>
      <li><strong>Destination Folder</strong> - Choose where to organize your dataset</li>
      <li><strong>Name</strong> - Give your dataset a descriptive name</li>
      <li><strong>Description</strong> (optional) - Add details about the external data source</li>
    </ul>
  </div>
</div>

<div class="step">
  <div class="step-number">6</div>
  <div class="content">Review your configuration and click <code>Save</code> to add the external dataset.</div>
</div>

:::tip Alternative Upload Method
You can also upload datasets directly while working in the [Map](../map/layers) interface for immediate use in your projects.
:::



## Working with a dataset

### Downloading a dataset

When downloading a spatial dataset, a dialog lets you choose:

- **Download Type** — the export file format (e.g. GeoPackage, GeoJSON, Shapefile).
- **Coordinate Reference System (CRS)** — the CRS to reproject the data into before download. GOAT automatically suggests CRS options based on the dataset's geographic extent: global options (WGS 84, Web Mercator) are always available, plus the matching UTM zone and any relevant national or regional CRS. The default is **WGS 84 (EPSG:4326)**.


<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/workspace/datasets/managing_datasets.png').default} alt="Dataset management options" style={{ maxHeight: "auto", maxWidth: "80%"}}/>
</div>

### Dataset metadata and preview

View detailed information about your datasets to better understand their content and structure. Access metadata by clicking directly on the dataset name.

The metadata view provides:

- <code>Summary</code> - Overview of dataset properties and statistics
- <code>Data</code> - Detailed view of all data fields and values  
- <code>Map</code> - Spatial visualization with interactive legend

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center'}}>
  <img src={require('/img/workspace/datasets/metadata.gif').default} alt="Metadata of the datasets in Workspace of GOAT" style={{ maxHeight: "auto", maxWidth: "auto", objectFit: "cover"}}/>
</div> 
