---
sidebar_position: 8
---

# Disjoint

This tool allows you to **filter input features to those that have no spatial intersection with any feature in an overlay layer**.

## 1. Explanation

The **Disjoint** tool returns the features from the input layer that do **not** spatially intersect any feature in the overlay layer: features that touch, overlap, contain, or are contained by overlay features are excluded; only features that fall entirely outside the overlay are kept.

Unlike Erase, Disjoint does **not** alter geometries. Each input feature is either kept as-is or dropped — its shape and attributes are preserved.

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>

  <img src={require('/img/toolbox/geoprocessing/disjoint.png').default} alt="Disjoint Tool" style={{ maxHeight: "400px", maxWidth: "400px", objectFit: "cover"}}/>

</div> 

## 2. Example use cases

- Find points of interest that lie outside a set of protected areas.
- Identify roads that do not pass through any flood zone.
- Select buildings that are not located within any planned construction site.

## 3. How to use the tool?

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Click on <code>Toolbox</code> <img src={require('/img/icons/toolbox.png').default} alt="Options" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/>. </div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Under the <code>Geoprocessing</code> menu, click on <code>Disjoint</code>.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Select the <code>Input layer</code> — the layer you want to filter.</div>
</div>

<div class="step">
  <div class="step-number">4</div>
  <div class="content">Select the <code>Overlay layer</code> — features in the input layer that intersect any feature in this layer will be excluded.</div>
</div>

<div class="step">
  <div class="step-number">5</div>
  <div class="content">Click <code>Run</code> to execute the disjoint filter. The result will be added to the map and contains only input features that are spatially disjoint from the overlay layer.</div>
</div>
