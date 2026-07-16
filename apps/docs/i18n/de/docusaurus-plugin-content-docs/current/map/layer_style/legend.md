---
sidebar_position: 5
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';



# Legende

**Legenden helfen Benutzern, die Symbologie und Bedeutung Ihrer Kartenlayer zu verstehen.** GOAT zeigt automatisch Legenden für alle sichtbaren Layer an, aber Sie können ihr Aussehen anpassen und beschreibende Beschriftungen hinzufügen, um Ihre Karten informativer zu machen.


## Wie man Layer-Legenden verwaltet

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Wählen Sie Ihren Layer und navigieren Sie zu <code>Layer Design</code> <img src={require('/img/icons/styling.png').default} alt="Styling Icon" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/> und finden Sie den <code>Legendenbereich</code>.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Aktivieren Sie das <code>Anzeigen</code> Kontrollkästchen, um <strong>die Legendenanzeige zu aktivieren oder zu deaktivieren</strong>.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Sie können ein <code>Untertitel</code> Feld hinzufügen, das <strong>den Inhalt des Layers erklärt</strong>. Der Untertitel erscheint unter dem Layer-Namen in der Legendenliste.</div>
</div>

<p></p>
<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center'}}>
  <img src={require('/img/map/styling/legend_de.webp').default} alt="Legendenkonfiguration mit Untertiteleinstellungen" style={{ maxHeight: "auto", maxWidth: "auto", objectFit: "cover"}}/>
</div>


## Benutzerdefinierte Legendenbezeichnungen für Farbstufen

Bei der attributbasierten Darstellung mit einer Farbskala (numerische oder benutzerdefinierte Klassifikation) können Sie jeder Farbstufe eine eigene Bezeichnung hinzufügen. Diese Bezeichnungen ersetzen die Rohdatenwerte in der Kartenlegende durch lesbare Beschreibungen.

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Öffnen Sie <code>Layer-Design</code> <img src={require('/img/icons/styling.png').default} alt="Styling-Symbol" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/> und navigieren Sie zum <code>Farbskala</code>-Panel unter dem attributbasierten Styling.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Unterhalb jeder Farbstufen-Zeile sehen Sie ein Textfeld mit dem Platzhalter <code>Legendenbezeichnung</code>. Geben Sie eine eigene Bezeichnung ein — z. B. <code>Niedrig</code>, <code>Mittel</code> oder <code>Hoch</code> — um den numerischen Wert in der Legende zu ersetzen.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Lassen Sie das Feld leer, um den standardmäßigen numerischen Wert in der Legende anzuzeigen.</div>
</div>

## Bewährte Praktiken

- **Verwenden Sie klare, beschreibende Untertitel**, die erklären, was der Layer darstellt
- **Halten Sie Untertitel prägnant**, aber informativ
- **Deaktivieren Sie Legenden** für Layer, die keine visuelle Erklärung benötigen (z.B. Referenzlayer)
- **Überprüfen Sie die Legendensichtbarkeit**, um eine Überfrachtung der Kartenoberfläche zu vermeiden
