---
sidebar_position: 5
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


# Legend

**Legends help users understand the symbology and meaning of your map layers.** GOAT automatically displays legends for all visible layers, but you can customize their appearance and add descriptive captions to make your maps more informative.

## How to manage layer legends

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Select your layer and navigate to <code>Layer design</code> <img src={require('/img/icons/styling.png').default} alt="Styling Icon" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/> and find the <code>Legend section</code></div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Toggle the <code>Show</code> checkbox to <strong>enable or disable the legend display</strong></div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">You can add a <code>Caption</code> field <strong>explaining the layer's content</strong>. The caption will appear below the layer name in the legend list</div>
</div>

<p></p>
<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center'}}>
  <img src={require('/img/map/styling/legend.webp').default} alt="Legend configuration with caption settings" style={{ maxHeight: "auto", maxWidth: "auto", objectFit: "cover"}}/>
</div>

## Custom legend labels for color steps

When using attribute-based styling with a color scale (numeric or custom classification), you can add a custom label to each color step. These labels replace the raw data values in the map legend with human-readable descriptions.

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Open <code>Layer design</code> <img src={require('/img/icons/styling.png').default} alt="Styling Icon" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/> and navigate to the <code>Color scale</code> panel under attribute-based styling.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Below each color step row you will see a text input with the placeholder <code>Legend label</code>. Type a custom label — for example <code>Low</code>, <code>Medium</code>, or <code>High</code> — to replace the numeric value in the legend.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Leave the field empty to show the default numeric value in the legend.</div>
</div>

## Best practices

- **Use clear, descriptive captions** that explain what the layer represents
- **Keep captions concise** but informative
- **Disable legends** for layers that don't need visual explanation (e.g., reference layers)
- **Review legend visibility** to avoid cluttering the map interface
