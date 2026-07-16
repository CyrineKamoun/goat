---
sidebar_position: 4
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


# Popup

**Popups display relevant information when users interact with map features.** This keeps your map clean while providing detailed information on demand. You can choose when to show the popup, add content blocks, and control how everything is presented.

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center'}}>
  <img src={require('/img/map/styling/popup.webp').default} alt="Popup displaying feature information" style={{ maxHeight: "auto", maxWidth: "auto", objectFit: "cover"}}/>
</div>

## How to configure popups

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Select your layer and navigate to <code>Layer design</code> <img src={require('/img/icons/styling.png').default} alt="Styling Icon" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/>, then open the <code>Popup</code> section and enable the toggle.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Set <code>Show popup on</code>: <code>On click</code>, <code>Only on hover</code>, or <code>On click and on hover</code>.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Under <code>Content</code>, click <code>+ Add block</code> to add content blocks. Available block types: <code>Field list</code>, <code>Text</code>, <code>Image</code>, <code>Button</code>, <code>Badge</code>, <code>Divider</code>.</div>
</div>

<div class="step">
  <div class="step-number">4</div>
  <div class="content">For a <code>Field list</code> block: choose <code>Table</code> or <code>List</code> layout, click <code>+ Add attribute</code> to select which fields to display, and optionally set <code>Collapse after</code> to limit the number of visible rows.</div>
</div>

<div class="step">
  <div class="step-number">5</div>
  <div class="content">Under <code>Appearance</code>, configure the following options:
  <ul>
    <li><code>Layout</code>: choose <code>Popup</code> or <code>Pinned</code></li>
    <li><code>Width</code>: set a fixed width in px, or leave as <code>Auto</code></li>
    <li><code>Max height</code>: set a maximum height in px to enable scrolling for long content</li>
    <li><code>Header</code>: choose <code>Standard</code>, <code>Compact</code>, or <code>None</code></li>
    <li><code>Highlight active feature</code>: toggle to highlight the selected feature on the map</li>
  </ul>
  </div>
</div>

## HTML mode

For full control over the popup design, switch to **HTML** mode under `Content`. This lets you write custom HTML and CSS to create rich, branded popups — with images, styled cards, custom fonts, and dynamic field values.

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Under <code>Content</code>, click the <code>HTML</code> tab.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Click <code>Edit</code> to open the HTML editor and write your custom markup.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Use <code>{"{{field_name}}"}</code> placeholders to inject feature attribute values dynamically into your HTML.</div>
</div>

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center'}}>
  <img src={require('/img/map/styling/popup_html.webp').default} alt="Custom HTML popup in GOAT" style={{ maxHeight: "400px", maxWidth: "100%", objectFit: "cover"}}/>
</div>

## Best practices

- **Choose relevant fields** that provide meaningful context to users
- **Use clear, descriptive names** instead of technical field names
- **Limit the number of fields** to avoid overwhelming users with information
- **Use collapse** to keep popups compact when showing many attributes
