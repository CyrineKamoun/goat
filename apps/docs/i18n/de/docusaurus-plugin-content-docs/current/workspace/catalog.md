---
sidebar_position: 4
---

# Katalog

Der **Daten-Katalog** ist Ihr Zugang zur Erkundung von Plan4Betters umfassender Sammlung hochwertiger [Geodaten](../further_reading/glossary#geospatial-data). **Diese kuratierte Bibliothek bietet zuverlässige, sofort einsatzfähige Daten von offiziellen Open-Data-Anbietern und anderen vertrauenswürdigen Quellen**, die es Ihnen ermöglichen, sofort mit der Analyse und Visualisierung in Ihren GOAT-**Projekten** zu beginnen. Aus dem **Katalog** können Sie:

- **Unsere Datensatz-Sammlung erkunden**, die mehrere thematische Bereiche und geografische Regionen umfasst
- **Durchsuchen und filtern** nach Stichwort, Ort, Kategorie, Anbieter, Lizenz und mehr
- **Häufig genutzte Datensätze speichern** und direkt zu einem Projekt hinzufügen

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/workspace/catalog/catalog_general_de.webp').default} alt="Daten-Katalog" style={{ maxHeight: "auto", maxWidth: "100%"}}/>
</div>

## Einen Datensatz finden

Öffnen Sie den **Katalog** über die Seitenleiste oder aus einem Projekt heraus über `+ Layer hinzufügen`. Die Seite listet alle Datensätze des Katalogs auf. Suche, Filter und Sortierung schränken diese Liste ein, und wenn Sie den Link der Seite kopieren, teilen Sie das Ergebnis — wer ihn öffnet, sieht dieselben Datensätze wie Sie.

### Suche

Tippen Sie in das Suchfeld, um Titel, Beschreibung und Schlagwörter eines Datensatzes zu durchsuchen.

Gesucht wird nach ganzen oder Teilwörtern, Groß- und Kleinschreibung spielt keine Rolle: `grünfläche` findet *Grünflächen*. Umgekehrt gilt das nicht — ein längeres Wort als das in den Daten findet nichts. Wählen Sie daher den kürzeren Wortstamm, wenn eine Suche leer bleibt.

### Filter

Die Seitenleiste schränkt die Liste ein nach:

- **Datenart** — Feature, Tabelle, Raster oder Datenpaket
- **Geometrie** — Punkt, Linie oder Polygon
- **Kategorie** — das Thema des Datensatzes: Environment, Landuse, Places, Transportation, People, Boundary, Basemap oder Sonstige
- **Anbieter** — wer den Datensatz bereitstellt
- **Lizenz** — zu welchen Bedingungen er veröffentlicht ist
- **Sprache** — die Sprache der Metadaten
- **Zeitraum** — ein `Von`- und `Bis`-Datum, um Daten zu einem bestimmten Zeitraum zu finden

Jede Option zeigt, wie viele Datensätze sie führen, und die Zahlen passen sich an, während Sie einschränken. Gesetzte Filter erscheinen als Chips über den Ergebnissen, sodass Sie einzelne entfernen können, ohne die Seitenleiste zu öffnen. `Zurücksetzen` entfernt alle.

### Nach Ort filtern {#raeumlicher-filter}

`Räumlichen Filter setzen` schränkt den Katalog auf Datensätze ein, die ein bestimmtes Gebiet abdecken. Dafür gibt es drei Wege:

- **Region** — nach Land, Bundesland oder Bezirk suchen
- **Punkt + Umkreis** — auf die Karte klicken, um einen Punkt zu setzen, dann einen Radius festlegen
- **Polygon** — auf die Karte klicken, um die Eckpunkte eines Gebiets zu zeichnen

Die Ergebnisse werden danach sortiert, wie viel eines Datensatzes in Ihrem Gebiet liegt. Datensätze, die es am besten abdecken, stehen oben.

### Sortierung und Ansichten

Sortieren Sie nach **Relevanz**, **Zuletzt aktualisiert** oder Titel **A–Z** / **Z–A**, und wechseln Sie zwischen **Liste** und **Kacheln**. Die Relevanz berücksichtigt Ihre Anzeigesprache: Bei deutscher Oberfläche stehen deutschsprachige Datensätze weiter oben.

### Häufig genutzte Datensätze speichern

Mit dem Stern auf einer Datensatz-Karte speichern Sie ihn. `Meine Favoriten anzeigen` schränkt den Katalog dann auf Ihre gespeicherten Datensätze ein. Diese bleiben über Sitzungen hinweg erhalten und stehen auch in der `+ Layer hinzufügen`-Auswahl im Projekt zur Verfügung.

## Was eine Datensatz-Seite zeigt

Klicken Sie auf einen Datensatz, um ihn zu öffnen. Der Reiter **Zusammenfassung** zeigt:

- **Beschreibung** von Inhalt und Umfang des Datensatzes
- **Typ**, **Geometrietyp**, **Kategorie**, **Sprache** und **Region**
- **Lizenz**, mit `Lizenz bei der Quelle ansehen`, sofern der Datengeber die Bedingungen verlinkt
- **Referenzjahr** und wann der Datensatz **Zuletzt aktualisiert** wurde
- **Schlagwörter**, mit denen der Datengeber ihn versehen hat
- **Datengeber**, mit Kontaktdaten, sofern vorhanden

Wenn ein Datensatz Daten zum Anzeigen hat, enthält der Reiter **Daten** **Beispieldaten** seiner Zeilen sowie eine Liste der **Spalten** mit Name und Typ. Die Beispieldaten sind begrenzt — sie sind ein Blick in die Daten, nicht der vollständige Datensatz.

Ein Datensatz aus mehreren Layern ist als `Datenpaket` gekennzeichnet und listet die **Layer in diesem Datenpaket**. Öffnen Sie einen davon, sehen Sie, zu welchem Datenpaket er gehört. Siehe [Datensatz-Typen](../data/dataset_types.md#datensätze-aus-mehreren-layern).

## Katalog-Daten zu einem Projekt hinzufügen

Datensätze werden aus einem Projekt heraus hinzugefügt — der Dialog `+ Layer hinzufügen` ist also der Ort dafür. Die Katalog-Seite für sich dient dem Finden und Speichern von Datensätzen; ihre Schaltfläche `Zum Projekt hinzufügen` ist noch nicht aktiv.


<div class="step">
  <div class="step-number">1</div>
  <div class="content">Öffnen Sie in Ihrem Projekt den Reiter <strong>Layer</strong> und klicken Sie auf <code>+ Layer hinzufügen</code>.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Wählen Sie den Reiter <code>Katalog</code>, um dieselben Datensätze mit derselben Suche und denselben Filtern zu durchsuchen.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Wählen Sie einen oder mehrere Datensätze aus und klicken Sie auf <code>Zum Projekt hinzufügen</code>.</div>
</div>

<p></p>

Wird ein Katalog-Datensatz zum ersten Mal hinzugefügt, bereitet GOAT eine Kopie davon für Ihr Projekt vor. Der Layer zeigt währenddessen `Daten werden vorbereitet …` und steht zur Verfügung, sobald das abgeschlossen ist.

:::info Katalog-Layer sind schreibgeschützt
Ein aus dem Katalog hinzugefügter Layer ist mit `Katalog · schreibgeschützt` gekennzeichnet. Sie können ihn wie jeden anderen Layer gestalten, filtern und analysieren, seine Zeilen und Spalten lassen sich jedoch nicht bearbeiten, und er kann nicht direkt heruntergeladen werden — Lizenz und Quelle bleiben beim Anbieter.
:::

Sobald eine neuere Fassung eines Datensatzes veröffentlicht ist, zeigt ein Layer in Ihrem Projekt `Aktualisierung im Katalog verfügbar`. So entscheiden Sie selbst, wann Sie die neuen Daten übernehmen.

:::tip Hinweis
Nach dem Hinzufügen des Layers können Sie [Filter](../map/filter.md "Datensatz filtern") anwenden, um große Datensätze auf die für Ihre Analyse benötigten Gebiete oder Attribute einzuschränken.
:::

## Was der Katalog enthält

Der Katalog deckt mehrere Themenbereiche ab. Dies sind einige der wichtigsten.

### Points of Interest (POIs)
Strategische Standorte von Annehmlichkeiten, Einrichtungen und Attraktionen, die für Erreichbarkeitsplanung und Stadtanalyse wesentlich sind, wie öffentliche Verkehrshaltestellen und -stationen, Einkaufszentren und Einzelhandelsstandorte, Tourismus- und Freizeiteinrichtungen, Lebensmittel- und Getränkebetriebe, Gesundheitseinrichtungen und Krankenhäuser, Bildungseinrichtungen und Schulen.

*Datenquellen:* [Overture Maps Foundation](https://overturemaps.org/), [OpenStreetMap (OSM)](https://wiki.openstreetmap.org/), Regierungsabteilungen, Krankenversicherungsanbieter und Einzelhandelsunternehmen. Zusätzliche Felderhebungen werden bei Bedarf durchgeführt.

### Bevölkerung und Gebäude
Detaillierte demografische **Daten**, die auf Gebäude- und lokale Ebenen disaggregiert sind, erweitert mit Landnutzungsinformationen für verbesserte Genauigkeit. Wir bieten Gebäudeebenen-Bevölkerungs**daten** für deutsche Bezirke und Gemeinden, lokale Bevölkerungs**daten** aus dem deutschen Zensus 2022 und europäische NUTS-3-Ebenen-Bevölkerungsstatistiken (Nomenklatur der territorialen Einheiten für die Statistik).

*Datenquellen:* [Deutscher Zensus 2022](https://ergebnisse.zensus2022.de/datenbank/online/), einzelne Gemeinden und Bezirke, und 3D-Stadtmodelle deutscher Bundesländer.

### Administrative Grenzen
Umfassende Grenzen-**Datensätze**, die Regierungs- und Verwaltungshoheitsgebiete auf mehreren Maßstäben definieren, wie Gemeindegrenzen, Bezirksgrenzen, Bundeslandgrenzen und Postleitzahl-Regionen.

*Datenquellen:* [Bundesamt für Kartographie und Geodäsie (BKG)](https://www.bkg.bund.de/) und [OpenStreetMap (OSM)](https://wiki.openstreetmap.org/).

## Datenqualität und Wartung

Plan4Better gewährleistet die Zuverlässigkeit und Aktualität der **Katalog**-**Daten** durch umfassende Datenmanagement-Prozesse:

### Datensammlung und -vorbereitung

Unser Datensammlungsprozess folgt strengen Standards, um Qualität und Zuverlässigkeit zu gewährleisten:

- **Quellenidentifizierung** - Wir priorisieren offizielle Open-Data-Portale und öffentlich verfügbare Initiativen
- **Format-Standardisierung** - Verschiedene **Formate** (Shapefiles, GeoJSON, usw.) werden zu konsistenten Schemata konvertiert
- **Datenintegration** - Mehrere **Datensätze** werden kombiniert und an lokale Kontexte durch Fusionsworkflows angepasst
- **Qualitätsvalidierung** - Umfassende Validierungsprozesse gewährleisten Genauigkeit und Zuverlässigkeit
- **Kontinuierliche Erweiterung** - Wir suchen aktiv und integrieren zusätzliche **Datensätze** basierend auf Benutzerbedürfnissen

### Update-Zeitplan

Um Datenaktualität und Relevanz zu erhalten:

- **Jährliche Updates** - Alle **Datensätze** werden mindestens einmal pro Jahr aktualisiert
- **Dynamische Daten** - Sich schnell verändernde **Daten** (POIs, öffentlicher Verkehr) erhalten häufigere Updates
- **On-Demand-Updates** - Kritische **Datensätze** können bei Bedarf basierend auf Benutzeranforderungen aktualisiert werden