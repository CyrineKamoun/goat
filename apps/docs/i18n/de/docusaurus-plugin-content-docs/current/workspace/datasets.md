---
sidebar_position: 4
---

# Datensätze

Die Datensätze-Seite ist Ihr **Datenmanagement-Hub, wo Sie alle Ihre räumlichen und nicht-räumlichen Daten in GOAT hochladen, organisieren und teilen können**. Dieser zentrale Workspace bietet eine organisierte Ansicht Ihrer Datensätze, kategorisiert in Persönliche Datensätze, Team-Datensätze und Organisationsweite geteilte Datensätze. Hier können Sie:

- **Neue Datensätze hinzufügen**
- **Datensätze filtern und organisieren** für bessere Datenstruktur und -verwaltung
- **Datensätze verwalten** durch Teilen, Verschieben, Löschen und andere Funktionen.
  
<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center'}}>
  <img src={require('/img/workspace/datasets/datasets_general_de.webp').default} alt="Datensätze-Seite im Workspace von GOAT" style={{ maxHeight: "auto", maxWidth: "100%", objectFit: "cover"}}/>
</div> 

## Datensätze hinzufügen

Sie können Datensätze auf zwei Arten zu GOAT hinzufügen: durch Hochladen von Dateien von Ihrem Computer oder durch Verbindung zu externen Datenquellen.

### Daten hochladen

GOAT unterstützt mehrere Dateiformate zum Hochladen: **GeoPackage**, **GeoJSON**, **Shapefile**, **KML**, **CSV**, **XLSX**, **ZIP**, **Parquet** und **COG**-Dateien.

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Navigieren Sie zur <code>Datensätze</code>-Seite über die Seitenleisten-Navigation.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Klicken Sie auf <code>+ Datensatz hinzufügen</code> und wählen Sie <code>Datensatz hochladen</code>.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Im Schritt <strong>Datei auswählen</strong> wählen Sie die Datei von Ihrem lokalen Gerät aus. Die unterstützten Formate sind unten im Dialog aufgelistet. Klicken Sie auf <code>Weiter</code>.</div>
</div>

<div class="step">
  <div class="step-number">4</div>
  <div class="content"><strong>Nur für CSV- und XLSX-Dateien — Schritt Vorschau &amp; Konfiguration:</strong> GOAT zeigt eine Vorschau Ihrer tabellarischen Daten an, damit Sie diese vor dem Import überprüfen können.
    <ul>
      <li><code>Arbeitsblatt</code> — Bei XLSX-Dateien mit mehreren Blättern wählen Sie aus, welches Blatt importiert werden soll.</li>
      <li><code>Erste Zeile ist Kopfzeile</code> — Aktivieren (Standard), wenn die erste Zeile Ihrer Datei Spaltennamen enthält. Deaktivieren, wenn die erste Zeile Daten enthält — Spaltennamen werden dann automatisch generiert und können später in den Layer-Einstellungen umbenannt werden.</li>
    </ul>
    Die Vorschau-Tabelle zeigt die ersten Zeilen Ihrer Datei. Klicken Sie auf <code>Weiter</code>, wenn Sie fertig sind.
  </div>
</div>

<div class="step">
  <div class="step-number">5</div>
  <div class="content">Im Schritt <strong>Ziel &amp; Metadaten</strong> konfigurieren Sie Ihren Datensatz:
    <ul>
      <li><strong>Zielordner</strong> — Wählen Sie, wo Sie Ihren Datensatz organisieren möchten</li>
      <li><strong>Name</strong> — Geben Sie Ihrem Datensatz einen beschreibenden Namen</li>
      <li><strong>Beschreibung</strong> (optional) — Fügen Sie Details über den Inhalt und Zweck Ihres Datensatzes hinzu</li>
    </ul>
    Klicken Sie auf <code>Weiter</code>.
  </div>
</div>

<div class="step">
  <div class="step-number">6</div>
  <div class="content">Überprüfen Sie Ihre Konfiguration im Schritt <strong>Bestätigung</strong> und klicken Sie auf <code>Hochladen</code>, um den Datensatz zu Ihrem Workspace hinzuzufügen.</div>
</div>

### Externe Datenquellen

Verbinden Sie sich mit externen Datendiensten einschließlich **Web Feature Service (WFS)**, **Web Map Service (WMS)**, **Web Map Tile Service (WMTS)**, **XYZ-Kacheln** und **Cloud Optimized GeoTIFF (COG)**.

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Navigieren Sie zur <code>Datensätze</code>-Seite über die Seitenleisten-Navigation.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Klicken Sie auf <code>+ Datensatz hinzufügen</code> und wählen Sie <code>Externer Datensatz</code>.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Geben Sie die URL des externen Datendienstes ein.</div>
</div>

<div class="step">
  <div class="step-number">4</div>
  <div class="content">Wählen Sie den spezifischen Layer aus, den Sie hinzufügen möchten, aus den verfügbaren Optionen und klicken Sie auf <code>Weiter</code>.</div>
</div>

<div class="step">
  <div class="step-number">5</div>
  <div class="content">
  <p>Konfigurieren Sie Ihren Datensatz:</p>
    <ul>
      <li><strong>Zielordner</strong> - Wählen Sie, wo Sie Ihren Datensatz organisieren möchten</li>
      <li><strong>Name</strong> - Geben Sie Ihrem Datensatz einen beschreibenden Namen</li>
      <li><strong>Beschreibung</strong> (optional) - Fügen Sie Details über die externe Datenquelle hinzu</li>
    </ul>
  </div>
</div>

<div class="step">
  <div class="step-number">6</div>
  <div class="content">Überprüfen Sie Ihre Konfiguration und klicken Sie auf <code>Speichern</code>, um den externen Datensatz hinzuzufügen.</div>
</div>

:::tip Alternative Upload-Methode
Sie können Datensätze auch direkt während der Arbeit in der [Karte](../map/layers) Oberfläche hochladen für die sofortige Verwendung in Ihren Projekten.
:::


## Datensätze filtern und organisieren

### Nach Datensatz-Typ filtern

Filtern Sie Ihre Datensätze einfach nach [Datensatz-Typ](../data/dataset_types "Was sind die Datensatz-Typen?"), um genau das zu finden, was Sie brauchen. Verfügbare Filter umfassen:

- **Features** - Räumliche Datensätze mit Punkten, Linien oder Polygonen
- **Tabellen** - Nicht-räumliche tabellarische Daten
- **Externe Bilder** - Rasterdaten aus externen Quellen
- **Externe Vektor-Kacheln** - Vektor-Kacheln aus externen Diensten

Klicken Sie auf das Filter-Symbol <img src={require('/img/icons/filter.png').default} alt="Filter-Symbol" style={{ maxHeight: "20px", maxWidth: "20px"}}/>, um Ihren gewünschten Datensatz-Typ auszuwählen.

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center'}}>
  <img src={require('/img/workspace/datasets/dataset_filter.gif').default} alt="Datensätze-Filterung im Workspace von GOAT" style={{ maxHeight: "auto", maxWidth: "auto", objectFit: "cover"}}/>
</div>

### Ordner erstellen und verwalten

Organisieren Sie Ihre Datensätze in Ordnern für bessere Struktur und einfachere Navigation.

**Um einen neuen Ordner zu erstellen:**

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Klicken Sie auf das <img src={require('/img/icons/folder.png').default} alt="Ordner-Symbol" style={{ maxHeight: "20px", maxWidth: "20px"}}/> Ordner-Symbol.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Geben Sie einen beschreibenden Namen für Ihren neuen Ordner ein</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Drücken Sie <code>Erstellen</code>, um den Ordner zu finalisieren</div>
</div>

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center'}}>
  <img src={require('/img/workspace/datasets/new_folder.gif').default} alt="Neue Ordner im Workspace von GOAT erstellen" style={{ maxHeight: "auto", maxWidth: "auto", objectFit: "cover"}}/>
</div>

<p></p>

**Um einen Ordner umzubenennen, zu teilen oder zu löschen:**

Klicken Sie auf das <img src={require('/img/icons/3dots.png').default} alt="Optionen" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/> <code>Weitere Optionen</code>-Symbol neben einem Ordner:

- <code>Umbenennen</code> — Ordnernamen ändern
- <code>Teilen</code> — Den Dialog <strong>Zugriff verwalten</strong> öffnen, der zwei Tabs enthält: <strong>Organisation</strong> (Zugriff für die gesamte Organisation) und <strong>Teams</strong> (Zugriff für bestimmte Teams). Diese schließen sich gegenseitig aus — wird der Organisation Zugriff gewährt, werden alle Team-Freigaben entfernt und umgekehrt. Für jeden Eintrag die Zugriffsebene auf <strong>Editor</strong> (kann bearbeiten) oder <strong>Viewer</strong> (nur lesen) setzen, oder <strong>Kein Zugriff</strong>, um den Zugriff zu entziehen. Auf <code>Speichern</code> klicken, um die Änderungen zu übernehmen. Alle Datensätze innerhalb eines geteilten Ordners erben automatisch den Ordnerzugriff — geerbter Zugriff wird in den Freigabe-Dialogen der jeweiligen Elemente als <code>Über Ordner</code> angezeigt und kann dort nicht direkt geändert werden.
- <code>Löschen</code> — Ordner entfernen

<p></p>

**Um Datensätze in Ordner zu verschieben:**

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Klicken Sie auf die drei Punkte <img src={require('/img/icons/3dots.png').default} alt="Optionen" style={{ maxHeight: "20px", maxWidth: "20px"}}/> neben Ihrem Datensatz</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Wählen Sie <code>In den Ordner verschieben</code> aus dem Menü</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Wählen Sie Ihren Zielordner aus dem Dropdown-Menü und drücken Sie <code>Verschieben</code>.</div>
</div>

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center'}}>
  <img src={require('/img/workspace/datasets/move_to_folder.gif').default} alt="Ihre Datensätze in die Ordner im Workspace von GOAT verschieben" style={{ maxHeight: "auto", maxWidth: "auto", objectFit: "cover"}}/>
</div>

## Datensatz-Verwaltung

Greifen Sie auf umfassende Datensatz-Verwaltungsoptionen über das <img src={require('/img/icons/3dots.png').default} alt="Optionen" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/> <code>Weitere Optionen</code>-Menü neben jedem Datensatz zu. Verfügbare Aktionen umfassen:

- <code>Metadaten bearbeiten</code> - Zugriff und Änderung von Datensatz-Metadaten
- <code>In den Ordner verschieben</code> - Datensatz-Speicherort neu organisieren
- <code>Herunterladen</code> - Datensätze auf Ihr lokales Gerät exportieren
- <code>Aktualisieren</code> - Datensatz mit neuen Daten aktualisieren
- <code>Teilen</code> - Zusammenarbeiten durch Teilen mit Teammitgliedern oder Organisation
- <code>Löschen</code> - Datensätze entfernen, die Sie nicht mehr benötigen

#### Datensätze herunterladen

Beim Herunterladen eines räumlichen Datensatzes können Sie im Dialog Folgendes auswählen:

- **Download-Typ** — das Exportdateiformat (z. B. GeoPackage, GeoJSON, Shapefile).
- **Koordinatenreferenzsystem** — das KRS, in das die Daten vor dem Download umprojiziert werden. GOAT schlägt automatisch KRS-Optionen basierend auf der geografischen Ausdehnung des Datensatzes vor: Globale Optionen (WGS 84, Web Mercator) sind immer verfügbar, zusätzlich die passende UTM-Zone sowie relevante nationale oder regionale KRS. Der Standardwert ist **WGS 84 (EPSG:4326)**.

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/workspace/datasets/managing_datasets.png').default} alt="Datensatz-Verwaltungsoptionen" style={{ maxHeight: "300px", maxWidth: "300px"}}/>
</div>

#### Datensatz-Metadaten und Vorschau

Zeigen Sie detaillierte Informationen über Ihre Datensätze an, um deren Inhalt und Struktur besser zu verstehen. Klicken Sie direkt auf den Datensatz-Namen, um die Metadaten-Ansicht zu öffnen.

Die Metadaten-Ansicht bietet:

- <code>Zusammenfassung</code> - Übersicht über Datensatz-Eigenschaften und Statistiken
- <code>Daten</code> - Detaillierte Ansicht aller Datenfelder und Werte
- <code>Karte</code> - Räumliche Visualisierung mit interaktiver Legende

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center'}}>
  <img src={require('/img/workspace/datasets/metadata.gif').default} alt="Metadaten der Datensätze im Workspace von GOAT" style={{ maxHeight: "auto", maxWidth: "auto", objectFit: "cover"}}/>
</div>