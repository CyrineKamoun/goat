---
sidebar_position: 3
---

# Inhalt

Auf der Seite **Inhalt** liegen Ihre Projekte, Datensätze und Vorlagen. Alles, worauf Sie zugreifen können, ist in **Bereiche** gegliedert — Ihren eigenen, die Ihrer Teams und den Ihrer Organisation. Inhalte gehören damit zu einem Bereich und nicht zu einer Person, und sie bleiben dort, wenn jemand hinzukommt oder das Team verlässt.

Auf der Seite Inhalt können Sie:

- **Alles in einem Bereich durchsuchen**, in Ordnern, die Sie selbst anlegen
- **Sehen, was andere mit Ihnen geteilt haben**, und was Sie zuletzt geöffnet haben
- **Teilen, verschieben, umbenennen, übertragen oder löschen**, wofür Sie verantwortlich sind
- **Wiederherstellen**, was Sie versehentlich gelöscht haben

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/workspace/content/content_general_de.webp').default} alt="Die Seite Inhalt in GOAT" style={{ maxHeight: "auto", maxWidth: "100%"}}/>
</div>

## Bereiche

Die linke Leiste zeigt die Bereiche, auf die Sie zugreifen können:

- **Meine Inhalte** — Ihr persönlicher Bereich. Was Sie erstellen, landet hier, sofern Sie es nicht woanders ablegen.
- **Team-Bereiche** — einer je Team, dem Sie angehören. Alle im Team können auf die Inhalte zugreifen.
- **Organisation** — für die gesamte Organisation freigegeben.

Darunter stehen drei Ansichten, die alle Bereiche zusammenfassen:

- **Mit mir geteilt** — alles, was andere mit Ihnen geteilt haben, an einer Stelle
- **Zuletzt bearbeitet** — was Sie zuletzt geöffnet haben, über alle erreichbaren Bereiche hinweg
- **Papierkorb** — gelöschte Inhalte, bis sie endgültig entfernt werden

Wählen Sie einen Bereich, um seine Inhalte zu sehen. Innerhalb eines Bereichs gruppieren **Ordner** die Inhalte so, wie es Ihnen passt.

## Sich zurechtfinden

Die Werkzeugleiste über den Inhalten bietet:

- **Suche** innerhalb des Bereichs, in dem Sie sich befinden
- Ansicht als **Kacheln** oder **Liste**
- **Filtern** nach Inhaltstyp
- **Sortieren**, etwa nach `Zuletzt aktualisiert`
- **Einzelheiten**, öffnet eine Leiste mit Angaben zum ausgewählten Element
- **Neu**, um ein Projekt anzulegen, einen Datensatz hochzuladen oder einen Ordner hinzuzufügen

## Inhalte verwalten

Wählen Sie ein Element — oder mehrere — aus, um damit zu arbeiten. Das Menü `Weitere Optionen` auf einer Karte und die Aktionsleiste bieten:

| Aktion | Was sie bewirkt |
|--------|-----------------|
| **Teilen** | Gibt anderen Zugriff. Sie bleiben Besitzer. |
| **Verschieben** | Legt das Element in einen anderen Ordner. |
| **Umbenennen** | Ändert den Namen. |
| **Rechte übertragen** | Übergibt das Element dauerhaft an jemand anderen. |
| **Löschen** | Verschiebt es in den Papierkorb. |

### Teilen und Übertragen sind zweierlei

**Teilen** gewährt Zugriff, während Sie Besitzer bleiben — sinnvoll, wenn Kolleginnen und Kollegen etwas sehen oder bearbeiten sollen, das weiterhin in Ihrer Verantwortung liegt.

**Rechte übertragen** verschiebt das Element dauerhaft in den Bereich einer anderen Person. Das ist der richtige Weg, wenn ein Projekt tatsächlich den Besitzer wechselt, etwa bei einer Übergabe vor dem Wechsel aus einem Team.

:::info Wer was sehen kann
Ein Element zeigt seine **Sichtbarkeit** — privat, mit einzelnen Personen geteilt, mit einem Team oder der Organisation geteilt, oder öffentlich. Ordner und Datenpakete können mit Teams und der Organisation geteilt werden.
:::

### Papierkorb und Wiederherstellen

Gelöschte Inhalte verschwinden nicht sofort: Sie landen im **Papierkorb**, aus dem der Besitzer sie **Wiederherstellen** kann. Sie bleiben dort, bis sie endgültig entfernt werden — ein versehentliches Löschen lässt sich also rückgängig machen.

## Inhalte hinzufügen

Über `Neu` auf der Seite Inhalt legen Sie ein Projekt an, laden einen Datensatz hoch oder erstellen einen Ordner. Ein Projekt lässt sich auch von der Startseite aus beginnen.

### Ein Projekt erstellen

Folgen Sie diesen einfachen Schritten, um ein neues **Projekt** zu erstellen:

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Öffnen Sie über die Seitenleiste die Seite <code>Inhalt</code>.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Klicken Sie auf <code>Neu</code> und wählen Sie eine Projekt-Option.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Geben Sie einen Namen ein und klicken Sie auf <code>Projekt erstellen</code>. Das Projekt wird in dem Ordner angelegt, in dem Sie sich gerade befinden.</div>
</div>

Um eine **Beschreibung** oder **Tags** hinzuzufügen oder das Projekt später umzubenennen, öffnen Sie dessen Menü `Weitere Optionen` und wählen `Metadaten bearbeiten`. Um es in einen anderen Ordner zu legen, verwenden Sie `Verschieben` aus demselben Menü.

### Ein Projekt importieren

Sie können eine zuvor exportierte GOAT-Projektdatei importieren:

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Klicken Sie auf <code>Neu</code> und wählen Sie die Option <code>Projekt importieren</code>.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Wählen Sie einen <strong>Projekt-Ordner</strong> und klicken Sie auf <code>Importieren</code>, um den Vorgang abzuschließen.</div>
</div>

### Einen Datensatz hochladen

GOAT unterstützt mehrere Dateiformate zum Hochladen: **GeoPackage**, **GeoJSON**, **Shapefile**, **KML**, **CSV**, **XLSX**, **ZIP**, **Parquet** und **COG**-Dateien sowie **GTFS**-Archive (`gtfs.zip`) für [ÖPNV-Netze](../data/dataset_types.md#öpnv-netze) und **Overture**-Archive (`overture.zip`) für [Straßennetze](../data/dataset_types.md#straßennetze).

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Öffnen Sie über die Seitenleiste die Seite <code>Inhalt</code>.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Klicken Sie auf <code>Neu</code> und wählen Sie <code>Datensatz</code>.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Im Schritt <strong>Datei auswählen</strong> wählen Sie die Datei von Ihrem lokalen Gerät aus oder ziehen sie per Drag-and-drop hinein. Die unterstützten Formate sind unten im Dialog aufgelistet. Klicken Sie auf <code>Weiter</code>.</div>
</div>

<div class="step">
  <div class="step-number">4</div>
  <div class="content">Im Schritt <strong>Ziel &amp; Metadaten</strong> konfigurieren Sie Ihren Datensatz:
    <ul>
      <li><strong>Name</strong> — Sie können den vorgeschlagenen Namen ändern</li>
      <li><strong>Zielordner</strong> — Wählen Sie, wo Sie Ihren Datensatz organisieren möchten</li>
      <li><strong>Beschreibung</strong> (optional) — Fügen Sie Details über den Inhalt und Zweck Ihres Datensatzes hinzu</li>
    </ul>
    Klicken Sie auf <code>Hochladen</code>.
  </div>
</div>

### Eine externe Quelle verbinden

Verbinden Sie sich mit externen Datendiensten einschließlich **Web Feature Service (WFS)**, **Web Map Service (WMS)**, **Web Map Tile Service (WMTS)**, **XYZ-Kacheln** und **Cloud Optimized GeoTIFF (COG)**.

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Öffnen Sie über die Seitenleiste die Seite <code>Inhalt</code>.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Klicken Sie auf <code>Neu</code> und wählen Sie <code>Datensatz</code>, und wählen Sie dann die Option für eine externe Quelle.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Geben Sie die URL des externen Datendienstes ein.</div>
</div>

<div class="step">
  <div class="step-number">4</div>
  <div class="content">Wählen Sie aus den verfügbaren Optionen den Layer aus, den Sie hinzufügen möchten, und klicken Sie auf <code>Weiter</code>.</div>
</div>

<div class="step">
  <div class="step-number">5</div>
  <div class="content">Konfigurieren Sie Ihren Datensatz:
    <ul>
      <li><strong>Name</strong> — Geben Sie Ihrem Datensatz einen beschreibenden Namen</li>
      <li><strong>Zielordner</strong> — Wählen Sie, wo Sie Ihren Datensatz organisieren möchten</li>
      <li><strong>Beschreibung</strong> (optional) — Fügen Sie Details über die externe Datenquelle hinzu</li>
    </ul>
  </div>
</div>

<div class="step">
  <div class="step-number">6</div>
  <div class="content">Überprüfen Sie Ihre Konfiguration und klicken Sie auf <code>Speichern</code>, um den externen Datensatz hinzuzufügen.</div>
</div>

:::tip Alternative Upload-Methode
Sie können Datensätze auch direkt während der Arbeit in der [Karte](../map/layers)-Oberfläche hochladen, um sie sofort in Ihren Projekten zu verwenden.
:::

## Mit einem Datensatz arbeiten

### Datensatz-Vorschau und Metadaten

Zeigen Sie detaillierte Informationen über Ihre Datensätze an, um deren Inhalt und Struktur besser zu verstehen.

- Öffnen Sie die Vorschau des Datensatzes, indem Sie ihn anklicken. Dort sehen Sie:
  - <code>Name</code>
  - <code>Beschreibung</code>
  - <code>Daten</code> - Detaillierte Ansicht aller Datenfelder und Werte
  - <code>Karte</code> - Räumliche Visualisierung mit interaktiver Legende

- Rufen Sie die Metadaten über das Menü <code>Weitere Optionen</code> neben dem Datensatz-Namen auf. Dort können Sie Name und Beschreibung sehen und bearbeiten oder Tags hinzufügen.

### Einen Datensatz herunterladen

Beim Herunterladen eines räumlichen Datensatzes können Sie im Dialog Folgendes auswählen:

- **Download-Typ** — das Exportdateiformat (z. B. GeoPackage, GeoJSON, Shapefile).
- **Koordinatenreferenzsystem (KRS)** — das KRS, in das die Daten vor dem Download umprojiziert werden. GOAT schlägt automatisch KRS-Optionen basierend auf der geografischen Ausdehnung des Datensatzes vor: Globale Optionen (WGS 84, Web Mercator) sind immer verfügbar, zusätzlich die passende UTM-Zone sowie relevante nationale oder regionale KRS. Der Standardwert ist **WGS 84 (EPSG:4326)**.
