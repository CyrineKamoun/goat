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
  <div class="content">Under <code>Appearance</code>, set the <code>Popup position</code> (<code>In place</code> or <code>Fixed</code>), and toggle <code>Show layer name header</code> and <code>Highlight active feature</code> as needed.</div>
</div>

## Best practices

- **Choose relevant fields** that provide meaningful context to users
- **Use clear, descriptive names** instead of technical field names
- **Limit the number of fields** to avoid overwhelming users with information
- **Use collapse** to keep popups compact when showing many attributes
