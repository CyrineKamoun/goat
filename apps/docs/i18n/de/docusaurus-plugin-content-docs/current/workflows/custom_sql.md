# Benutzerdefinierte SQL

:::warning Erweiterte Funktion
Dies ist eine erweiterte Funktion, die für Benutzer mit SQL-Kenntnissen gedacht ist. Falsche Abfragen können dazu führen, dass Workflows fehlschlagen oder unerwartete Ergebnisse liefern. Wenn Sie Hilfe beim Schreiben von SQL-Abfragen benötigen, können Sie KI-Assistenten verwenden, um Code zu generieren und zu erklären.
:::

Das **Benutzerdefinierte SQL**-Werkzeug ermöglicht es Ihnen, benutzerdefinierte SQL-Abfragen für Datenanalysen direkt innerhalb Ihrer Workflows zu schreiben. Diese mächtige Funktion ermöglicht erweiterte Datenverarbeitung, die über GOATs eingebaute Werkzeuge hinausgeht.

## Übersicht

Das Benutzerdefinierte SQL-Werkzeug verbindet sich mit GOATs DuckDB Backend und gibt Ihnen direkten Zugriff auf die Abfrage Ihrer Datensätze mit SQL-Syntax. Sie können:

- Komplexe analytische Abfragen ausführen
- Mehrere Datensätze verknüpfen
- Aggregationen und statistische Berechnungen durchführen  
- Abgeleitete Datensätze mit benutzerdefinierter Logik erstellen
- Auf erweiterte räumliche Funktionen zugreifen

## Verwendung der benutzerdefinierten SQL

### Hinzufügen des Werkzeugs

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Ziehen Sie aus dem <strong>Werkzeuge Panel</strong> das Werkzeug <strong>Benutzerdefinierte SQL</strong> auf Ihre Workflow-Leinwand.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Verbinden Sie Eingabe-Datensatz-Knoten oder andere Werkzeuge, um Datenquellen für Ihre Abfrage bereitzustellen.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Klicken Sie auf den Benutzerdefinierte SQL-Knoten, um das <strong>Konfigurations-Panel</strong> rechts zu öffnen.</div>
</div>

### Schreiben von SQL-Abfragen

Im Konfigurations-Panel finden Sie einen SQL-Editor, in dem Sie Ihre benutzerdefinierte Abfrage schreiben können:

```sql
SELECT 
  h.*,
  p.population_density,
  ST_Distance(h.geom, p.geom) AS distance_to_center
FROM input_1 h
JOIN input_2 p ON ST_Intersects(h.geom, p.geom)
WHERE p.population_density > 1000
ORDER BY distance_to_center
```

#### Eingabe-Referenzen

- **input_1, input_2, input_3...**: Referenzieren Sie Ihre verbundenen Datensätze mit diesen Tabellennamen
- Die Nummer entspricht der Verbindungsreihenfolge am Knoten
- Sie können bis zu 3 Eingabedatensätze pro Benutzerdefinierte SQL-Knoten verbinden

#### Verfügbare Funktionen

Das Benutzerdefinierte SQL-Werkzeug unterstützt Standard-SQL-Funktionen plus räumliche Operationen:

**Räumliche Funktionen:**
- `ST_Distance()` - Berechnet Entfernungen zwischen Geometrien
- `ST_Intersects()` - Prüft, ob sich Geometrien überschneiden
- `ST_Within()` - Testet, ob eine Geometrie innerhalb einer anderen liegt
- `ST_Buffer()` - Erstellt Puffer um Geometrien
- `ST_Area()` - Berechnet Geometriefläche
- `ST_Length()` - Berechnet Linienlänge

**Analytische Funktionen:**
- `AVG()`, `SUM()`, `COUNT()` - Statistische Aggregationen
- `PERCENTILE_CONT()` - Berechnet Perzentile
- `ROW_NUMBER()`, `RANK()` - Fensterfunktionen
- `CASE WHEN` - Bedingte Logik

### Abfrage-Validierung

<div class="step">
  <div class="step-number">1</div>
  <div class="content"><strong>Syntax-Überprüfung</strong>: Der Editor hebt Syntaxfehler hervor, während Sie tippen.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content"><strong>Abfrage validieren</strong>: Klicken Sie auf die Schaltfläche <code>Validieren</code>, um zu prüfen, ob Ihre Abfrage erfolgreich ausgeführt wird.</div>
</div>

## Beispiele

### Grundlegende Filterung und Auswahl
```sql
-- Gebäude innerhalb eines bestimmten Gebiets auswählen
SELECT building_type, height, geom
FROM input_1 
WHERE building_type = 'residential' 
  AND height > 10
```

### Räumliche Join-Analyse
```sql
-- Alle Annehmlichkeiten innerhalb von 500m von Haltestellen finden
SELECT 
  a.name as amenity_name,
  a.amenity_type,
  t.stop_name,
  ST_Distance(a.geom, t.geom) as distance
FROM input_1 a
JOIN input_2 t ON ST_DWithin(a.geom, t.geom, 500)
ORDER BY distance
```

### Aggregation nach Gebiet
```sql
-- Punkte nach Verwaltungsgebiet zählen
SELECT 
  admin.district_name,
  COUNT(points.*) as point_count,
  admin.geom
FROM input_1 points
RIGHT JOIN input_2 admin 
  ON ST_Within(points.geom, admin.geom)
GROUP BY admin.district_name, admin.geom
```

## Bewährte Praktiken

:::tip Performance
- Verwenden Sie räumliche Indizes, indem Sie geometrische Prädikate in WHERE-Klauseln einschließen
- Begrenzen Sie Ergebnisse während der Entwicklung mit `LIMIT 100`
- Testen Sie zuerst mit kleinen Datensätzen, dann skalieren Sie auf
:::

:::warning Datentypen
- Stellen Sie sicher, dass Geometriespalten ordnungsgemäß für räumliche Operationen formatiert sind
- Wandeln Sie Datentypen explizit um, wenn Sie verschiedene Datensätze verknüpfen
- Prüfen Sie auf NULL-Werte in kritischen Spalten
:::

### Abfrage-Optimierung

**Verwenden Sie räumliche Prädikate**: Schließen Sie immer räumliche Filter wie `ST_DWithin()` ein, wenn möglich, um räumliche Indizes zu nutzen.

**Spaltenauswahl**: Wählen Sie nur die Spalten aus, die Sie benötigen, anstatt `SELECT *` zu verwenden.

**Ordnungsgemäße Joins**: Verwenden Sie geeignete Join-Typen (INNER, LEFT, RIGHT) basierend auf Ihren Analyseanforderungen.

### Fehlerbehandlung

Häufige Probleme und Lösungen:

- **"Tabelle nicht gefunden"**: Stellen Sie sicher, dass Eingabedatensätze ordnungsgemäß verbunden sind
- **"Spalte existiert nicht"**: Überprüfen Sie Spaltennamen in Ihren Eingabedatensätzen  
- **"Geometriefehler"**: Überprüfen Sie, ob Geometriespalten gültig und ordnungsgemäß formatiert sind
- **"Timeout"**: Teilen Sie komplexe Abfragen in kleinere Schritte auf oder erhöhen Sie das Timeout

## Ausgabe und Integration

Das Benutzerdefinierte SQL-Werkzeug erstellt eine neue temporäre Ebene, die Ihre Abfrageergebnisse enthält. Sie können:

- Die Ausgabe mit anderen Workflow-Werkzeugen für weitere Analysen verbinden
- Einen Export-Knoten hinzufügen, um Ergebnisse als permanenten Datensatz zu speichern
- Die Ergebnisse in Visualisierungen und Styling verwenden

:::info Variablen-Unterstützung
Benutzerdefinierte SQL-Abfragen unterstützen [Workflow-Variablen](variables.md) mit der Syntax `{{@variable_name}}` für parametrisierte Abfragen.
:::

## Einschränkungen

- Maximal 3 Eingabedatensätze pro Benutzerdefinierte SQL-Knoten
- Abfragen müssen mindestens eine Geometriespalte für die Kartierung zurückgeben
- Einige erweiterte DuckDB-Funktionen sind möglicherweise nicht verfügbar
- Die Abfrageausführungszeit ist durch das konfigurierte Timeout begrenzt

Für komplexere Analyseanforderungen erwägen Sie die Verwendung mehrerer Benutzerdefinierte SQL-Knoten oder die Kombination mit anderen Workflow-Werkzeugen.
