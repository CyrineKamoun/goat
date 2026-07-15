---
sidebar_position: 7
---

# Grundkarten

**Grundkarten bilden die Kartenbasis Ihres Projekts** und geben Ihren Daten geografischen Kontext — Straßen, Gelände, Satellitenbilder oder eine einfarbige Fläche. GOAT unterstützt alle Anbieter, die eine Style-JSON-URL (Vektor) oder eine XYZ-Kachel-URL (Raster) bereitstellen.

## So fügen Sie eine eigene Grundkarte hinzu

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Klicken Sie in der Kartenoberfläche auf die Schaltfläche <img src={require('/img/icons/map.png').default} alt="Grundkarte Symbol" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/> <code>Grundkarte</code> in den Kartennavigations-Steuerelementen.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Klicken Sie am unteren Ende des Panels auf <code>+ Neue Basemap hinzufügen</code>.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Wählen Sie den Tab <code>Basemap</code>. Wählen Sie <code>Vektor</code> für Style-JSON-Quellen oder <code>Raster</code> für XYZ-Kachelquellen.</div>
</div>

<div class="step">
  <div class="step-number">4</div>
  <div class="content">Geben Sie die <strong>Basemap-URL</strong>, einen <strong>Titel</strong> und optional eine Beschreibung sowie eine Vorschaubild-URL ein. Klicken Sie auf <code>Basemap hinzufügen</code>, um zu speichern.</div>
</div>

:::tip Einfarbiger Hintergrund
Verwenden Sie den Tab <strong>Einfarbig</strong> anstelle einer URL, um eine Volltonfarbe als Kartenhintergrund festzulegen — nützlich für Drucklayouts oder minimalistische Dashboards.
:::

## Häufig verwendete Anbieter

### Mapbox

Mapbox bietet eine große Auswahl an Kartenstilen. Erstellen Sie ein kostenloses Konto auf [mapbox.com](https://www.mapbox.com), um Ihren Zugriffstoken zu erhalten.

**Beispiel-URL:**
```
https://api.mapbox.com/styles/v1/mapbox/streets-v12/tiles/256/{z}/{x}/{y}?access_token={IHR_ZUGRIFFSTOKEN}
```

:::info Sichtbarkeit des Zugriffstokens
Grundkarten, die zu einem **geteilten oder öffentlichen Projekt** hinzugefügt werden, machen Ihren Mapbox-Zugriffstoken für Betrachter sichtbar. Beschränken Sie den Token in Ihrem Mapbox-Konto auf Ihre Domain, um unbefugte Nutzung zu verhindern.
:::

---

### MapTiler

MapTiler bietet hochwertige Vektor-Grundkarten, die nahtlos mit der Karten-Engine von GOAT funktionieren. Erstellen Sie ein kostenloses Konto auf [maptiler.com](https://www.maptiler.com), um Ihren API-Schlüssel zu erhalten.

**Beispiel-URL:**
```
https://api.maptiler.com/maps/streets-v2/style.json?key={IHR_API_SCHLÜSSEL}
```

:::info Sichtbarkeit des API-Schlüssels
Ihr MapTiler-API-Schlüssel ist in geteilten Projekten sichtbar. Verwenden Sie die Schlüsselbeschränkungseinstellungen in der MapTiler Cloud, um die Nutzung auf Ihre Domain zu begrenzen.
:::

---

### Esri / ArcGIS

Esri bietet eine Vielzahl professioneller Grundkarten — kein Konto oder API-Schlüssel erforderlich.

**Beispiel-URL:**
```
https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}
```

---

### OpenStreetMap

OpenStreetMap (OSM) bietet kostenlose, gemeinschaftlich gepflegte Grundkarten — kein Konto oder API-Schlüssel erforderlich.

**Beispiel-URL:**
```
https://tile.openstreetmap.org/{z}/{x}/{y}.png
```

:::info Nutzungsrichtlinien
Die Kachelserver von OpenStreetMap sind für geringen Datenverkehr ausgelegt. Für produktive oder stark frequentierte Projekte empfiehlt sich ein gehosteter Anbieter wie [MapTiler](#maptiler), der OSM-basierte Kartenstile mit besserer Zuverlässigkeit anbietet.
:::

---

### Carto Dark Matter

Der Dark-Matter-Stil von Carto bietet eine dunkle, minimalistische Grundkarte, die sich besonders für datenintensive Karten eignet, bei denen helle Datenvisualisierungen hervorstechen sollen — kein Konto oder API-Schlüssel erforderlich.

**Beispiel-URL:**
```
https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json
```
