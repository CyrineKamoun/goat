---
sidebar_position: 4
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


# Popup

**Popups zeigen relevante Informationen an, wenn Benutzer mit Karten-Features interagieren.** Sie halten Ihre Karte übersichtlich und stellen gleichzeitig detaillierte Informationen auf Abruf bereit. Sie können vollständig anpassen, welcher Inhalt angezeigt wird und wie er dargestellt wird.

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center'}}>
  <img src={require('/img/map/styling/popup_de.webp').default} alt="Popup zeigt Feature-Informationen an" style={{ maxHeight: "auto", maxWidth: "auto", objectFit: "cover"}}/>
</div>

## Wie man Popups konfiguriert

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Wählen Sie Ihren Layer und navigieren Sie zu <code>Layer-Design</code> <img src={require('/img/icons/styling.png').default} alt="Styling Icon" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/>, öffnen Sie dann den Abschnitt <code>Popup</code> und aktivieren Sie den Schalter.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Wählen Sie unter <code>Popup zeigen</code>: <code>Bei Klick</code>, <code>Nur bei Hover</code> oder <code>Bei Klick und Hover</code>.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Klicken Sie unter <code>Inhalt</code> auf <code>+ Block hinzufügen</code>, um Inhaltsblöcke hinzuzufügen. Verfügbare Blocktypen: <code>Feldliste</code>, <code>Text</code>, <code>Bild</code>, <code>Schaltfläche</code>, <code>Abzeichen</code>, <code>Trennlinie</code>.</div>
</div>

<div class="step">
  <div class="step-number">4</div>
  <div class="content">Für einen <code>Feldliste</code>-Block: Wählen Sie das Layout <code>Tabelle</code> oder <code>Liste</code>, klicken Sie auf <code>+ Attribut hinzufügen</code>, um die anzuzeigenden Felder auszuwählen, und setzen Sie optional <code>Einklappen nach</code>, um die Anzahl der sichtbaren Zeilen zu begrenzen.</div>
</div>

<div class="step">
  <div class="step-number">5</div>
  <div class="content">Legen Sie unter <code>Aussehen</code> die <code>Popup-Position</code> (<code>Am Objekt</code> oder <code>Fixiert</code>) fest und aktivieren Sie bei Bedarf <code>Layernamen-Kopfzeile anzeigen</code> und <code>Aktives Objekt hervorheben</code>.</div>
</div>

## Bewährte Praktiken

- **Wählen Sie relevante Felder** aus, die Benutzern bedeutungsvollen Kontext bieten
- **Verwenden Sie klare, beschreibende Namen** anstatt technischer Feldnamen
- **Begrenzen Sie die Anzahl der Felder**, um Benutzer nicht mit Informationen zu überfordern
- **Nutzen Sie Einklappen**, um Popups bei vielen Attributen kompakt zu halten
