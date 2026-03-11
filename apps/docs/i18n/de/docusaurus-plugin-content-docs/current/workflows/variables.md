# Variablen

**Workflow-Variablen** ermöglichen es Ihnen, wiederverwendbare, parametrisierte [Workflows](../further_reading/glossary.md#workflows) zu erstellen, indem Sie dynamische Werte definieren, die geändert werden können, ohne die Workflow-Struktur zu modifizieren. Diese mächtige Funktion macht Ihre Analyse anpassungsfähig und teilbar.

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/workflows/variables.webp').default} alt="Kartenoberfläche Übersicht" style={{ maxHeight: "auto", maxWidth: "auto", objectFit: "cover"}}/>
</div> 

## Übersicht

Variablen ermöglichen es Ihnen:

- Vorlagen für wiederholte Analysen mit verschiedenen Parametern zu erstellen  
- Workflows zu erstellen, die andere ohne technische Kenntnisse anpassen können
- Verschiedene Szenarien zu testen, indem Sie Schlüsselwerte einfach ändern
- Standardisierte analytische Prozesse projektübergreifend zu teilen

Variablen verwenden die Syntax `{{@variable_name}}` und können in den meisten Werkzeugparametern in Ihrem Workflow verwendet werden.

## Variablentypen

GOAT unterstützt mehrere Variablentypen, um verschiedenen Parameteranforderungen zu entsprechen:

### Text-Variablen
Für Zeichenkettenwerte wie Datensatznamen, Beschriftungen oder Filterkriterien:
```
Variablenname: district_name
Typ: Text  
Standardwert: Innenstadt
Verwendung: {{@district_name}}
```

### Zahlen-Variablen  
Für numerische Parameter wie Entfernungen, Schwellenwerte oder Berechnungen:
```
Variablenname: buffer_distance
Typ: Zahl
Standardwert: 500
Verwendung: {{@buffer_distance}}
```

### Boolean-Variablen
Für wahr/falsch-Optionen und Schalter:
```  
Variablenname: include_residential  
Typ: Boolean
Standardwert: true
Verwendung: {{@include_residential}}
```

### Listen-Variablen
Für Dropdown-Auswahlen aus vordefinierten Optionen:
```
Variablenname: amenity_type
Typ: Liste
Optionen: [restaurant, school, hospital, park]  
Standardwert: restaurant
Verwendung: {{@amenity_type}}
```

## Erstellen von Variablen

### Verwenden des Variablen-Panels

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Öffnen Sie das <strong>Variablen-Panel</strong> auf der rechten Seite der Workflow-Benutzeroberfläche.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Klicken Sie auf die Schaltfläche <strong>Variable hinzufügen</strong>, um eine neue Variable zu erstellen.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Konfigurieren Sie die Variableneigenschaften:
    <ul>
      <li><strong>Name</strong>: Verwenden Sie beschreibende Namen wie <code>search_radius</code> oder <code>poi_type</code></li>
      <li><strong>Typ</strong>: Wählen Sie den entsprechenden Datentyp aus</li>  
      <li><strong>Standardwert</strong>: Setzen Sie einen vernünftigen Standard für den Parameter</li>
    </ul>
  </div>
</div>

<div class="step">
  <div class="step-number">4</div>
  <div class="content">Klicken Sie auf <strong>Speichern</strong>, um die Variable zu Ihrem Workflow hinzuzufügen.</div>
</div>

### Variablen-Management

**Variablen bearbeiten**: Klicken Sie auf eine beliebige Variable im Variablen-Panel, um ihre Eigenschaften zu ändern.

**Variablen löschen**: Verwenden Sie die Löschen-Schaltfläche, um ungebrauchte Variablen zu entfernen.

## Verwendung von Variablen in Workflows

### In Werkzeug-Parametern

Variablen können in vielen Konfigurationsfeldern verwendet werden:

**Puffer-Analyse**: Setzen Sie dynamische Pufferentfernungen
```
Pufferentfernung: {{@analysis_radius}}
```

**Filter**: Erstellen Sie flexible Filterkriterien  
```
Annehmlichkeitstyp: {{@selected_amenity}}
Bevölkerungsschwellenwert: {{@min_population}}
```

**Benutzerdefinierte SQL**: Parametrisieren Sie Abfragen
```sql
SELECT * FROM input_1 
WHERE category = '{{@category_filter}}'
  AND value > {{@threshold_value}}
```

### Variablen-Syntax-Regeln

- **Format**: Verwenden Sie immer die Syntax `{{@variable_name}}`
- **Groß-/Kleinschreibung beachten**: Variablennamen sind groß-/kleinschreibungssensitiv
- **Keine Leerzeichen**: Verwenden Sie Unterstriche anstelle von Leerzeichen (z.B. `max_distance` nicht `max distance`)  
- **Beschreibende Namen**: Verwenden Sie klare, beschreibende Namen, die den Zweck des Parameters erklären

## Bewährte Praktiken

### Variablen-Design

:::tip Aussagekräftige Namen
Verwenden Sie beschreibende Variablennamen, die ihren Zweck klar angeben: `search_radius` anstelle von `radius`, `poi_type` anstelle von `type`.
:::

:::tip Vernünftige Standards  
Setzen Sie Standardwerte, die für häufige Anwendungsfälle funktionieren, sodass Benutzer Workflows sofort ausführen können, während Anpassungen weiterhin möglich sind.
:::

## Einschränkungen

- Variablennamen müssen innerhalb eines Workflows eindeutig sein
- Einige erweiterte Werkzeugparameter unterstützen möglicherweise keine Variablen
- Variablenwerte werden mit dem Workflow gespeichert, nicht global
- Listen-Variablen sind auf vordefinierte Optionen beschränkt