---
sidebar_position: 3
---

import MathJax from 'react-mathjax';

# Heatmap - Closest Average
The Heatmap - Closest Average indicator **produces a color-coded map visualizing the average travel time to points, such as POIs, from surrounding areas.**

<div style={{ display: 'flex', justifyContent: 'center' }}>
<iframe width="674" height="378" src="https://www.youtube.com/embed/-nBXd-LAqZA?si=3dUu-gFsVM1KjS4e&amp;start=46" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>
</div>

## 1. Explanation

The heatmap displays a color-coded hexagonal grid **showing average travel times to destinations (opportunities) using real-world transport networks.** You can specify the **routing type**, **opportunity layer**, **number of destinations** and **travel time limit** to generate the visualization.

- The **Opportunity layer contains point-based destination data** (POIs, transit stations, schools, amenities, or custom data) **that you want to analyze accessibility to**. You can use multiple opportunity layers and they will be combined into a unified heatmap.

- The **Number of destinations sets the calculation of average travel time to only the *n* closest opportunities**. This creates more targeted accessibility analysis.

**Key difference:** Heatmaps show *access* from many origins to specific destinations, while catchment areas show *reach* from specific origins to many destinations.


:::info

Heatmap computation is available across **over 30 European countries** for `Walk`, `Bicycle`, `Pedelec`, and `Car`. For `Public Transport`, Germany, Switzerland, and the Haut-Rhin region of France are supported. If you need analyses beyond these regions, feel free to [contact us](https://plan4better.de/en/contact/) and we'll discuss further options.

:::

## 2. Example use cases

 - Do residents in certain areas have longer average travel times to amenities than others?

 - How does the average travel time to amenities vary across different modes of transport?

 - How does the average travel time vary across different types of amenities?
 
 - If standards require that a minimum number of amenities be accessible within a certain travel time, which areas meet these standards?

## 3. How to use the indicator?

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Click on <code>Toolbox</code> <img src={require('/img/icons/toolbox.png').default} alt="Options" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/> .</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Under the <code>Accessibility Indicators</code> menu, click on <code>Heatmap Closest Average</code>.</div>
</div>

### Routing

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Pick the <code>Transport mode</code> you would like to use for the heatmap.</div>
</div>

| Mode | Considers | Speed assumed |
|------|-----------|---------------|
| Walk | All paths accessible by foot | 5 km/h |
| Bicycle | All paths accessible by bicycle (surface, smoothness, slope) | 15 km/h |
| Pedelec | All paths accessible by pedelec (surface, smoothness) | 23 km/h |
| Car | All paths accessible by car (speed limits, one-way restrictions) | — |


### Opportunities

<div class="step">
  <div class="step-number">4</div>
  <div class="content">Select your <code>Input Layer</code> from the drop-down menu. This can be any previously created layer containing point-based data.</div>
</div>

<div class="step">
  <div class="step-number">5</div>
  <div class="content">Choose a <code>Travel Time Limit</code> for your heatmap. This will be used in the context of your previously selected <code>Transport mode</code>.</div>
</div>

:::tip Hint

Need help choosing a suitable travel time limit for various common amenities? The ["Standort-Werkzeug"](https://www.chemnitz.de/chemnitz/media/unsere-stadt/verkehr/verkehrsplanung/vep2040_standortwerkzeug.pdf) of the City of Chemnitz can provide helpful guidance.

:::

<div class="step">
  <div class="step-number">6</div>
  <div class="content">Specify the <code>Number of destinations</code> which should be considered while computing the average travel time.</div>
</div>

<div class="step">
  <div class="step-number">7</div>
  <div class="content">Optionally, click <code>+ Add Opportunities</code> to include additional opportunity layers. Each layer can have different travel time limits and destination counts for multi-criteria analysis.</div>
</div>

<div class="step">
  <div class="step-number">8</div>
  <div class="content">Optionally, expand <code>Advanced Options</code> and select a <code>Reference Area</code> — a polygon layer that defines the full study area. When set, the heatmap extends to cover all H3 cells within that polygon, with cells outside the computed reach shown as <code>NULL</code> to expose coverage gaps and underserved areas.</div>
</div>

### Result Layer

<div class="step">
  <div class="step-number">9</div>
  <div class="content">Set the <code>Result layer name</code> for the output heatmap layer.</div>
</div>

<div class="step">
  <div class="step-number">10</div>
  <div class="content">Click <code>Run</code> to start the calculation of the heatmap.</div>
</div>

### Results

Once the calculation is complete, a result layer will be added to the map. Clicking on any of the **heatmap's hexagonal cells will reveal the computed average travel time value for this cell.**

<div style={{ display: 'flex', justifyContent: 'center' }}>
<img src={require('/img/toolbox/accessibility_indicators/heatmaps/closest_average_based/clst-avg-calculation.gif').default} alt="Closest Average Heatmap Calculation Result in GOAT" style={{ maxHeight: "auto", maxWidth: "80%"}}/>
</div>

## 4. Technical details

### Calculation

**After combining all opportunity layers** (for example, schools, shops, or parks), the tool **creates a grid made of hexagonal cells around the area**. **It only includes cells where at least one opportunity can be reached based on the selected** **routing type** (e.g., walking, cycling) and **travel time limit** (e.g., 15 minutes).

Then, for each cell, it calculates the average travel time to the **nearest n destinations** (as set in the settings).

The formula for average travel time is:

<MathJax.Provider>
  <div style={{ marginTop: '20px', fontSize: '24px' }}>
    <MathJax.Node formula={"\\overline{t}_i = \\frac{\\sum_{j=1}^{n} t_{ij}}{n}"} />
  </div>
</MathJax.Provider>

For each cell (i), the tool adds up the travel times (tij) to all reachable opportunities (j), up to n of them, and divides by n to get the average travel time.

### Classification
In order to classify the accessibility levels that were computed for each grid cell, a classification based on quantiles is used by default. However, various other classification methods may be used instead. Read more in the **[Data Classification Methods](../../map/layer_style/style/attribute_based_styling#data-classification-methods)** section of the *Attribute-based Styling* page.

### Visualization 

Heatmaps in GOAT utilize **[Uber's H3 grid-based](../../further_reading/glossary#h3-grid)** solution for efficient computation and easy-to-understand visualization. Behind the scenes, a pre-computed travel time matrix for each *routing type* utilizes this solution and is queried and further processed in real-time to compute accessibility and produce a final heatmap.

The resolution and dimensions of the hexagonal grid used depend on the selected *routing type*:

| Mode | Resolution | Average hexagon area | Average hexagon edge length |
|------|-----------|----------------------|-----------------------------|
| Walk | 10 | 11,285.6 m² | 65.9 m |
| Bicycle | 9 | 78,999.4 m² | 174.4 m |
| Pedelec | 9 | 78,999.4 m² | 174.4 m |
| Car | 8 | 552,995.7 m² | 461.4 m |


:::tip Hint

For further insights into the Routing algorithm, visit [Routing](../../category/routing). In addition, you can check this [Publication](https://doi.org/10.1016/j.jtrangeo.2021.103080).


:::

### Example of calculation

The following examples illustrate the computation of a closest-average-based heatmap for the same opportunities, with a varying `Number of destinations` value.

<div style={{ display: 'flex', justifyContent: 'center' }}>
<img src={require('/img/toolbox/accessibility_indicators/heatmaps/closest_average_based/cls-avg-destinations.png').default} alt="Closest Average Heatmaps for different destinations" style={{ maxHeight: "auto", maxWidth: "80%"}}/>
</div>

<p></p>

In the first example, the average travel time is computed considering only the closest destination, while in the second example, the closest 5 destinations are considered.
