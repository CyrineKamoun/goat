---
sidebar_position: 3
sidebar_label: Netz-Datensätze
---

# Netz-Datensätze

## Die Grundlage hinter GOATs Indikatoren


GOATs Erreichbarkeitsindikatoren und Analysetools basieren auf hochwertigen Netz-Datensätzen, die im Hintergrund arbeiten. **GOAT bringt integrierte Netze für ÖPNV und Straßen mit, und Sie können [eigene Netze importieren](#eigene-netze-importieren), wenn eine Analyse auf Daten laufen soll, die Sie selbst verwalten.**

Das Verständnis dieser zugrundeliegenden Datensätze hilft Ihnen:
- **Die Datenqualität zu kennen**, die Sie von GOATs Indikatoren erwarten können
- **Die geografische Abdeckung** verschiedener Analyse-Werkzeuge zu verstehen
- **Ergebnisse zu interpretieren** mit Kenntnis der Datenquellen

:::info Netze vs. eigene Datensätze
Auf dieser Seite geht es um die **Netz-Datensätze**, die hinter GOATs Routing- und Erreichbarkeitsanalysen stehen. Wie Sie eigene Datensätze hochladen oder fertige verwenden, erfahren Sie unter [Inhalt](../workspace/content.md) und im [Katalog](../workspace/catalog.md).
:::

## Die integrierten Netze von GOAT

GOAT umfasst umfassende Netzwerk-Datensätze, die alle routing-basierten Erreichbarkeitsindikatoren und Analyse-Werkzeuge antreiben.

### Öffentliches Verkehrsnetz

Unser öffentliches Verkehrsnetz deckt mehrere Verkehrsmittel ab, einschließlich Bus, Tram, U-Bahn, Bahn und Fähre. Dieses Netzwerk ermöglicht GOATs [Öffentlicher Verkehr](../routing/public_transport) Routing-Funktionen.

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/data/data_basis/pt_network_banner.png').default} alt="Öffentliches Verkehrsnetz" style={{ maxHeight: "auto", maxWidth: "auto", objectFit: "cover"}}/>
</div>

**Was enthalten ist:**
- **Haltestellen**: Namen, Standorte, Typen und Barrierefreiheitsinformationen
- **Routen**: Service-Typen, Barrierefreiheitsdetails und Routeninformationen  
- **Fahrpläne**: Abfahrtszeiten, Service-Häufigkeit und Betriebstage
- **Umsteigeverbindungen**: Umsteigeangaben und Bahnhofsverbindungen
- **Fahrtmuster**: Haltestellensequenzen und Zeitinformationen
- **Routenverläufe**: Geospatiale Darstellung von Verkehrslinien

**Datenquellen:**
- **Deutschland**: [DELFI](https://www.delfi.de/) - Deutschlands nationale Datenplattform für öffentliche Verkehrsmittel
- **Straßenebene-Daten**: [OpenStreetMap (OSM)](https://wiki.openstreetmap.org/) - Für Bahnhofszugang, Fußgängerverbindungen und multimodales Routing

**Wie wir die Daten verarbeiten:**
1. **Import**: Daten werden im [GTFS (General Transit Feed Specification)](https://gtfs.org/)-Format gesammelt
2. **Überprüfen & Korrigieren**: Wir validieren Haltestellenbeziehungen, Bahnsteigverbindungen und Service-Typ-Klassifizierungen
3. **Optimieren**: Netzwerke werden optimiert, um nur die repräsentativsten Service-Muster für jede Route zu enthalten
4. **Fahrplan-Typen**: Die Analyse unterstützt drei Tagestypen - **Werktag** (typischerweise Dienstag), **Samstag** und **Sonntag**

### Straßennetzwerk und Topografie

Unser Straßennetzwerk repräsentiert reale Verkehrsinfrastruktur einschließlich Straßen, Autobahnen, Fahrradwegen und Fußwegen. Dies ermöglicht GOATs [Zu Fuß](../routing/walking), [Fahrrad](../routing/bicycle), [Pedelec](../routing/bicycle) und [Auto](../routing/car) Routing.

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/data/data_basis/street_network_banner.png').default} alt="Straßennetzwerk" style={{ maxHeight: "auto", maxWidth: "auto", objectFit: "cover"}}/>
</div>

**Netzwerk-Komponenten:**
- **Segmente (Kanten)**: Kontinuierliche Wegabschnitte zwischen Kreuzungen
- **Kreuzungen (Knoten)**: Punkte, an denen sich verschiedene Wege treffen oder kreuzen

**Datenquellen:**
- **Straßennetze**: [Overture Maps Foundation](https://overturemaps.org/) - Hochwertige, europaweite Verkehrsdaten
- **Höhendaten**: [Copernicus](https://www.copernicus.eu/en) Digitales Höhenmodell (DEM) für genaue Steigungsberechnungen

**Verarbeitungsworkflow:**
1. **Datenimport**: Straßennetzwerkdaten werden im Geoparquet-Format aus Overture Maps' [Transportation theme](https://docs.overturemaps.org/guides/transportation/) importiert
2. **Höhenverarbeitung**: Europäische DEM-Kacheln werden verarbeitet, um topografische Informationen zu extrahieren
3. **Räumliche Indexierung**: Netzwerksegmente werden mit [Ubers H3-Gittersystem](../further_reading/glossary#h3-grid) für effiziente Verarbeitung organisiert
4. **Steigungsberechnung**: Oberflächengfäle und Steigungswiderstand werden für jedes Straßensegment berechnet
5. **Attributanalyse**: Straßenklassifizierungen, Geschwindigkeitsbegrenzungen, Abbiegebeschränkungen und Einbahnstraßenbezeichnungen werden identifiziert und standardisiert
6. **Geschwindigkeitsbegrenzungs-Interpolation**: Fehlende Geschwindigkeitsbegrenzungen werden basierend auf Straßentyp und modalen Geschwindigkeiten geschätzt

## Eigene Netze importieren

Die oben beschriebenen Netze sind die integrierten Netze von GOAT und werden standardmäßig verwendet. Sie können auch **eigene Netze importieren** — ein Netz, das Sie selbst pflegen, eine Region, die Sie mit Ihren eigenen Daten analysieren möchten, oder ein geplantes Netz, das Sie testen möchten, bevor es gebaut wird.

Nach dem Import wird Ihr Netz genauso verwendet wie das integrierte: Die Routing- und Erreichbarkeitswerkzeuge bieten es neben `Standard (Europa)` an, und Sie wählen aus, auf welchem Netz eine Analyse laufen soll.

### Eigenes Straßennetz

Ein importiertes Straßennetz lässt sich **auf der Karte bearbeiten**: Zeichnen Sie eine Straße, und GOAT teilt und verbindet die Topologie, pflegt die Nodes und erstellt die Routing-Daten aus Ihren Änderungen neu. So können Sie eine geplante Verbindung testen — eine neue Brücke, eine gesperrte Straße, einen Radweg — und eine Analyse darauf erneut ausführen.

### Eigenes ÖPNV-Netz

Da GOAT den Weg zu und von jeder Haltestelle routet, muss ein ÖPNV-Netz mit einem **Straßennetz verknüpft** sein. Sie wählen dieses Netz beim Hochladen aus, entweder eines Ihrer eigenen oder das integrierte `Standard (Europa)`. Das Straßennetz muss also bereits vorhanden sein. Durch die Verknüpfung mit Ihrem eigenen Straßennetz berücksichtigt eine Fahrplananalyse auch die Straßen, die Sie geändert haben.

Aus welchen Dateiformaten diese Netze importiert werden und aus welchen Layern sie bestehen, erfahren Sie unter [Datensatz-Typen](./dataset_types.md#straßennetze). Die Importschritte finden Sie unter [Inhalte hinzufügen](../workspace/content.md#inhalte-hinzufügen).