---
sidebar_position: 2
sidebar_label: Feldtypen
---

# Feldtypen

Jede Spalte in einem Feature-Datensatz oder einer Tabelle hat einen **Feldtyp**. Er bestimmt, wie GOAT die Werte speichert, wie sie angezeigt werden und welche Werkzeuge die Spalte verwenden können. Diese Seite erklärt die Feldtypen, die GOAT unterstützt.

## Speicherung räumlicher Daten

GOAT trennt die **Metadaten** eines Datensatzes von den **Daten** selbst:

- **Metadaten** — Name, Beschreibung, Felddefinitionen, Gestaltung und Freigabeeinstellungen des Datensatzes — liegen in einer **PostgreSQL**-Datenbank.
- **Daten** — die Geometrien und Attributwerte — werden in **DuckLake** gespeichert, einem analytischen Speicher auf Basis von Parquet-Dateien im Objektspeicher. Dadurch kann GOAT auch große Datensätze schnell abfragen und analysieren.

Alle Geometrien werden im Koordinatenbezugssystem **EPSG:4326** gespeichert (Längen- und Breitengrad in Grad). Gemessen wird jedoch nicht in Grad: **Area**-, **Perimeter**- und **Length**-Spalten werden auf der gekrümmten Erdoberfläche berechnet, sodass ihre Werte echte Meter sind und auch über große Gebiete hinweg genau bleiben.

## Unterstützte Feldtypen

| Feldtyp | Was er enthält | Beispiele |
|---------|----------------|-----------|
| **Text** | Beliebigen Text | Straßennamen, Kategorien, Beschreibungen |
| **Number** | Ganze Zahlen oder Dezimalzahlen | `1`, `-5`, `3.14`, `45.67` |
| **Boolean** | Wahr- oder Falsch-Werte | `true`, `false` |
| **Date** | Einen Zeitpunkt | `2024-01-15 14:30:00` |
| **Area** | Die Fläche eines Polygons | `1,25 ha`, `340 m²` |
| **Perimeter** | Die Umrisslänge eines Polygons | `820 m` |
| **Length** | Die Länge einer Linie | `1,4 km` |
| **Formula** | Einen aus anderen Spalten berechneten Wert | `population / area` |

### Spalten, die Sie selbst befüllen

**Text**, **Number**, **Boolean** und **Date** enthalten die Werte, die Sie eingeben oder importieren. Sie können sie jederzeit zu einem Datensatz hinzufügen, und es sind die Typen, die beim Anlegen eines neuen Datensatzes zur Verfügung stehen.

Eine **Number**-Spalte lässt sich für die Anzeige formatieren. Die Formatierung ändert nur, wie der Wert in GOAT aussieht — der gespeicherte Wert bleibt unverändert, und Werkzeuge rechnen weiterhin mit der vollständigen Zahl.

| Einstellung | Wirkung auf `1234567.891` |
|-------------|---------------------------|
| **Nachkommastellen** | `2` rundet die Anzeige auf `1234567,89`; `auto` überlässt GOAT die Wahl |
| **Tausendertrennzeichen anzeigen** | `1.234.567,891` |
| **Große Zahlen abkürzen** | `1,2 Mio.` |
| **Vorzeichen immer anzeigen** | `+1234567,891`, damit positive Werte genauso deutlich gekennzeichnet sind wie negative |

Dieselben Einstellungen stehen für **Area**-, **Perimeter**- und **Length**-Spalten zur Verfügung. Dort können Sie zusätzlich die **Einheit** wählen, in der der Wert angezeigt wird — `m²`, `ha` oder `km²` für Area und `m` oder `km` für Perimeter und Length. Bei `auto` wählt GOAT eine zum Wert passende Einheit.

GOAT zeigt dabei eine Live-Vorschau, sodass Sie das Ergebnis vor dem Speichern sehen.

### Spalten, die GOAT berechnet

**Area**, **Perimeter**, **Length** und **Formula** sind **berechnete** Spalten: GOAT leitet ihre Werte ab und hält sie aktuell, daher lassen sie sich nicht von Hand bearbeiten. Sie werden angeboten, wenn Sie die Felder eines bestehenden Datensatzes bearbeiten — ein neuer Datensatz enthält noch keine Daten, aus denen sie berechnet werden könnten.

**Formula** lässt sich zu jedem Datensatz hinzufügen. Welche Messgrößen zur Verfügung stehen, hängt von der Geometrie Ihres Datensatzes ab, denn nicht jede Form lässt sich gleich messen:

- **Polygone** — Area und Perimeter
- **Linien** — Length
- **Punkte** — keine davon, da ein Punkt keine Ausdehnung hat
- **Tabellen** (ohne Geometrie) — keine davon

Eine **Formula**-Spalte wird über einen Ausdruck definiert, den Sie im Formel-Editor schreiben. GOAT berechnet sie neu, sobald sich die zugrunde liegenden Daten ändern.

## Zulässige Werte einer Spalte einschränken

**Text**- und **Number**-Spalten können eine Liste **zulässiger Werte** führen. Alle, die den Datensatz bearbeiten, wählen dann aus dieser Liste, statt frei zu tippen. Das hält Kategorien einheitlich — kein `bus stop`, `Bus Stop` und `bus_stop` mehr in derselben Spalte.

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Öffnen Sie in der Datentabelle die <strong>Text</strong>- oder <strong>Number</strong>-Spalte, die Sie einschränken möchten: entweder über <code>Felder bearbeiten</code> und Auswahl aus der Liste, oder über das <img src={require('/img/icons/3dots.png').default} alt="Weitere Optionen" style={{ maxHeight: "16px", maxWidth: "16px", verticalAlign: "middle"}}/> Menü der Spaltenüberschrift und <code>Feld bearbeiten</code>.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Geben Sie unter <code>Zulässige Werte</code> einen Wert ein und drücken Sie <code>Enter</code>. Wiederholen Sie das für jeden Wert, den Sie zulassen möchten.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Sobald die Liste mindestens einen Wert enthält, erscheint der Schalter <code>Andere Werte zulassen</code>. Aktivieren Sie ihn, wenn die Liste ein Vorschlag und keine Vorgabe sein soll.</div>
</div>

<div class="step">
  <div class="step-number">4</div>
  <div class="content">Klicken Sie auf <code>Speichern</code>.</div>
</div>

Eine leere Liste lässt beliebige Werte zu — so beginnt jede Spalte.

:::info Reservierte Spaltennamen
Einige Namen verwendet GOAT intern; sie können nicht an eine Spalte vergeben werden, darunter `id`, `geometry` und `geom`. Geben Sie einen davon ein, markiert GOAT das Feld sofort und lässt das Speichern erst zu, wenn Sie den Namen ändern.
:::

## Feldtypen anzeigen

GOAT kennzeichnet jede Spalte mit einem kleinen **Feldtyp-Symbol**: `A` für Text, `123` für Number und ein Symbol für die übrigen Typen. Fahren Sie mit der Maus darüber, um den Namen des Feldtyps zu sehen.

Das Symbol erscheint überall dort, wo Spalten aufgelistet sind — in der Datentabelle, in der Feldliste der Layer-Einstellungen, in Spaltenfiltern und in den Feldauswahlen von Werkzeugen und Formel-Editor.

So sehen Sie es in der Datentabelle:

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Klicken Sie auf <img src={require('/img/icons/3dots.png').default} alt="Weitere Optionen" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/> <code>Weitere Optionen</code> bei Ihrem Layer.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Wählen Sie <code>Daten anzeigen</code> aus dem Menü.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">In der Datentabelle wird das Feldtyp-Symbol neben jedem Spaltennamen angezeigt.</div>
</div>

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/data/view_data_de.webp').default} alt="Weitere Optionen" style={{ maxHeight: "auto", maxWidth: "auto", objectFit: "cover"}}/>
  <p style={{ textAlign: 'center', fontStyle: 'italic', marginTop: '8px', color: '#666' }}>Die Option „Daten anzeigen“ aufrufen</p>
</div>

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/data/data-table.webp').default} alt="Datentabelle mit Feldtypen" style={{ maxHeight: "auto", maxWidth: "auto", objectFit: "cover"}}/>
  <p style={{ textAlign: 'center', fontStyle: 'italic', marginTop: '8px', color: '#666' }}>Datentabelle mit dem Feldtyp neben jedem Spaltennamen</p>
</div>
