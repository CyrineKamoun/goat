---
sidebar_position: 4
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


# Popup

**Popups zeigen relevante Informationen an, wenn Benutzer mit Karten-Features interagieren.** Dies hält Ihre Karte übersichtlich und stellt gleichzeitig detaillierte Informationen auf Abruf bereit. Sie können wählen, wann das Popup angezeigt wird, Inhaltsblöcke hinzufügen und die Darstellung vollständig steuern.

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
  <div class="content">Konfigurieren Sie unter <code>Aussehen</code> die folgenden Optionen:
  <ul>
    <li><code>Layout</code>: Wählen Sie <code>Popup</code> oder <code>Angeheftet</code></li>
    <li><code>Breite</code>: Geben Sie eine feste Breite in px an oder lassen Sie <code>Auto</code></li>
    <li><code>Maximale Höhe</code>: Legen Sie eine maximale Höhe in px fest, um bei langen Inhalten scrollen zu können</li>
    <li><code>Kopfzeile</code>: Wählen Sie <code>Standard</code>, <code>Kompakt</code> oder <code>Keine</code></li>
    <li><code>Aktives Objekt hervorheben</code>: Aktivieren Sie den Schalter, um das ausgewählte Objekt auf der Karte hervorzuheben</li>
  </ul>
  </div>
</div>

## HTML-Modus

Für vollständige Kontrolle über das Popup-Design wechseln Sie unter `Inhalt` in den **HTML**-Modus. Damit können Sie benutzerdefiniertes HTML und CSS schreiben, um ansprechende, individuell gestaltete Popups zu erstellen — mit Bildern, gestalteten Karten, eigenen Schriften und dynamischen Feldwerten.

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Klicken Sie unter <code>Inhalt</code> auf den Tab <code>HTML</code>.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Klicken Sie auf <code>Bearbeiten</code>, um den HTML-Editor zu öffnen und Ihr benutzerdefiniertes Markup zu schreiben.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Verwenden Sie <code>{"{{feldname}}"}</code>-Platzhalter, um Feature-Attributwerte dynamisch in Ihr HTML einzufügen.</div>
</div>

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center'}}>
  <img src={require('/img/map/styling/popup_html_de.webp').default} alt="Benutzerdefiniertes HTML-Popup in GOAT" style={{ maxHeight: "400px", maxWidth: "100%", objectFit: "cover"}}/>
</div>

## Bewährte Praktiken

- **Wählen Sie relevante Felder** aus, die Benutzern bedeutungsvollen Kontext bieten
- **Verwenden Sie klare, beschreibende Namen** anstatt technischer Feldnamen
- **Begrenzen Sie die Anzahl der Felder**, um Benutzer nicht mit Informationen zu überfordern
- **Nutzen Sie Einklappen**, um Popups bei vielen Attributen kompakt zu halten
