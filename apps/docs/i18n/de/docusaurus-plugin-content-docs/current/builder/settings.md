---
sidebar_position: 3
---

# Einstellungen

Im Einstellungsbereich können Sie **die Kartensteuerung, das Branding, Social Sharing und das Interaktionsverhalten Ihres Dashboards konfigurieren**. Wenn Sie eine Funktion deaktivieren, steht sie im Betrachter-Modus nicht zur Verfügung.

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/builder/interface_settings_de.webp').default} alt="Dashboard-Einstellungen in GOAT" style={{ maxHeight: "auto", maxWidth: "auto", objectFit: "cover"}}/>
</div>

## Karte

- `Werkzeugleiste` — zeigt die obere Leiste mit dem GOAT-Logo, Projektnamen, Zuletzt-gespeichert-Zeitstempel und Projektinfo im Betrachter-Modus.
- `Maßstabsleiste` — zeigt eine Skala auf der Karte, mit der Abstände von einem Punkt zum anderen gemessen werden können.

### Steuerungslayout

Steuert die Position der Kartenelemente im Betrachter-Modus. Drei Positionen stehen zur Verfügung: **Oben-links**, **Oben-rechts** und **Unten-rechts**. Klicken Sie bei jeder Position auf `+`, um ein Steuerelement hinzuzufügen, ziehen Sie Chips zum Umsortieren, und klicken Sie auf `×` an einem Chip, um ihn zu entfernen.

Verfügbare Steuerelemente:

| Steuerelement | Beschreibung |
|---|---|
| `Standortsuche` | Suchleiste, um einen Ort auf der Karte anzuspringen |
| `Messen` | Werkzeug zum Messen von Distanzen und Flächen auf der Karte |
| `Zoom-Steuerung` | Schaltflächen zum Hinein- und Herauszoomen |
| `Auswahl Hintergrundkarten` | Dropdown zum Wechseln der Hintergrundkarte |
| `Vollbildmodus` | Vollbildmodus ein-/ausschalten |
| `Meinen Standort finden` | Karte auf den aktuellen Standort des Betrachters zentrieren |
| `Projektinfo` | Projektinfo-Panel anzeigen |

### Erlaubte Hintergrundkarten

Legen Sie fest, welche Hintergrundkarten Betrachter verwenden können. Wählen Sie eine oder mehrere aus dem Dropdown aus — Betrachter sehen nur die hier aktivierten Optionen. Wird nur angezeigt, wenn `Auswahl Hintergrundkarten` an einer Position platziert ist.

### Zoom-Grenzen

Begrenzen, wie weit Dashboard-Betrachter hinein- und herauszoomen können. Mit einem Bereichsregler legen Sie den minimalen und maximalen Zoomwert fest (0–22). Der aktuelle Kartenzoom wird als Markierung auf dem Regler angezeigt.

---

## Branding

Passen Sie die visuelle Identität Ihres Dashboards für den Betrachter-Modus an.

- `Schriftart` — wählen Sie eine Schriftart aus dem Dropdown. Wählen Sie `Eigene…`, um eine **Schriftdatei-URL** und einen **Schriftart**-Namen für eine eigene Schriftart einzugeben.
- `Primärfarbe` — legen Sie die Hauptakzentfarbe für Schaltflächen und Hervorhebungen fest.
- `Symbolfarbe` — legen Sie die Farbe für Symbole im Dashboard fest.
- `Schriftfarbe` — legen Sie die Textfarbe im gesamten Dashboard fest.
- `Favicon` — laden Sie ein benutzerdefiniertes Browser-Tab-Symbol hoch. Klicken Sie auf `×`, um es zu entfernen.

---

## Social Sharing

Legen Sie fest, wie Ihr Dashboard beim Teilen in sozialen Medien oder Messenger-Apps erscheint.

- **Vorschaubild** — Bild hierher ziehen oder klicken, um es hochzuladen (empfohlen: 1200×630 Pixel). Falls nicht gesetzt, wird das Standard-GOAT-Vorschaubild verwendet.
- **Beschreibung** — fügen Sie eine kurze Beschreibung (bis zu 300 Zeichen) hinzu, die in Social-Media-Vorschauen und Suchergebnissen verwendet wird.

---

## Allgemein

- `Sprache` — legen Sie die Anzeigesprache des Dashboards fest. Optionen: `Automatisch (Browser-Standard)`, `English`, `Deutsch`.

---

## Interaktionen

Klicken Sie auf `Interaktionen verwalten`, um den Interaktionseditor zu öffnen. Interaktionen verknüpfen Dashboard-Elemente miteinander — zum Beispiel kann das Klicken auf eine Layer-Gruppe den aktiven Tab in einem Widget wechseln. Klicken Sie auf `Interaktion hinzufügen`, um eine neue Interaktion zu erstellen.

---

::::note

Mit `Zurücksetzen` am unteren Ende des Panels können Sie alle Einstellungen auf die Standardwerte zurücksetzen.

::::
