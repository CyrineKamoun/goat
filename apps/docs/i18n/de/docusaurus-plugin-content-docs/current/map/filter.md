---
sidebar_position: 5
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


# Filter


**Filter begrenzt die Datensichtbarkeit auf Ihrer Karte** durch logische Ausdrücke (z.B. Supermärkte mit bestimmten Namen) oder räumliche Ausdrücke (z.B. Punkte innerhalb eines Begrenzungsrahmens). **Das Filter-Tool ermöglicht es Ihnen, sich auf relevante Informationen zu konzentrieren, ohne die ursprünglichen Daten zu verändern.** Es funktioniert mit **Punkt-Layern** und **Polygon-Layern**, die `Zahlen`-, `String`-, `Datum`- und `Boolean`-Datentypen enthalten.

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>

  <Video src={require('/img/map/filter/filter_clicking.mp4').default} alt="Filter tool in GOAT" style={{ maxHeight: "auto", maxWidth: "auto", objectFit: "cover"}}/>

</div> 


## Wie benutzt man den Filter?

### Einzelausdruck-Filterung

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Wählen Sie Ihren Layer aus und navigieren Sie zu <code>Layer-Design</code> <img src={require('/img/icons/styling.png').default} alt="Styling-Symbol" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/> und finden Sie den <code>Filter-Bereich</code>.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Der <code>Aktive Layer</code>-Selektor <strong>zeigt den aktuell ausgewählten Layer</strong> für die Filterung an.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Klicken Sie auf <code>+ Ausdruck hinzufügen</code>, um <strong>einen neuen Filterausdruck hinzuzufügen</strong>.</div>
</div>

<div class="step">
  <div class="step-number">4</div>
  <div class="content">Wählen Sie <code>Logischer Ausdruck</code> oder <code>Räumlicher Ausdruck</code>, um <strong>Ihren Filtertyp zu definieren</strong>.</div>
</div>

<Tabs>
  <TabItem value="Logical expression" label="Logischer Ausdruck" default className="tabItemBox">

<div class="step">
  <div class="step-number">5</div>
  <div class="content">Wählen Sie das <code>Feld</code> (Attribut), um <strong>zu filtern</strong>.</div>
</div>

<div class="step">
  <div class="step-number">6</div>
  <div class="content">Wählen Sie den <code>Operator</code>. Verfügbare Optionen variieren je nach Datentyp (<code>Zahl</code>, <code>String</code>, <code>Datum</code> und <code>Boolean</code>).</div>
</div>

<div style={{ display: 'flex', justifyContent: 'center' }}>

| Ausdrücke für `Zahl` | Ausdrücke für `String` |
| -------------------- | ---------------------- |
| ist                  | ist                    |
| ist nicht            | ist nicht              |
| enthält              | enthält                |
| schließt aus         | schließt aus           |
| ist leer             | ist leer               |
| ist nicht leer       | ist nicht leer         |
| ist mindestens       | beginnt mit            |
| ist weniger als      | endet mit              |
| ist höchstens        | enthält den Text       |
| ist größer als       | enthält den Text nicht |
| liegt zwischen       | ist leerer String      |
|                      | ist kein leerer String |

</div>

<div style={{ display: 'flex', justifyContent: 'center' }}>

| Ausdrücke für `Datum` | Ausdrücke für `Boolean` |
| --------------------- | ----------------------- |
| ist am                | ist wahr                |
| ist nicht am          | ist falsch              |
| ist vorher            | ist leer                |
| ist danach            | ist nicht leer          |
| im letzten            |                         |
| nicht im letzten      |                         |
| liegt zwischen        |                         |
| liegt nicht dazwischen |                        |

</div>

:::tip Hinweis
Bei `Datum`-Feldern wählen Sie ein Datum über den **Datumsauswähler**. **"liegt zwischen"** verwendet zwei Daten (**Von** und **Bis**), und **"im letzten"** erwartet eine **Anzahl von Tagen**. Bei `Boolean`-Feldern legt der Operator die Bedingung bereits fest, sodass kein Wert erforderlich ist.
:::

:::tip Hinweis
Für die Ausdrücke **"enthält"** und **"schließt aus"** können mehrere Werte ausgewählt werden.
:::

<div class="step">
  <div class="step-number">7</div>
  <div class="content">Legen Sie Ihre Filterkriterien fest. Die Karte wird <strong>automatisch aktualisiert</strong> und zeigt ein Filtersymbol auf dem gefilterten Layer an.</div>
</div>

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/map/filter/filter_atlayer_de.webp').default} alt="Filter Result in GOAT" style={{ maxHeight: "auto", maxWidth: "auto", objectFit: "cover"}}/>
</div> 
</TabItem>

<TabItem value="Spatial expression" label="Räumlicher Ausdruck" default className="tabItemBox">
<div class="step">
  <div class="step-number">5</div>
  <div class="content">Wählen Sie die <code>Schnittmethode</code> für die <strong>räumliche Begrenzung</strong>.</div>
</div>

<Tabs>
  <TabItem value="Map extent" label="Kartenausdehnung" default className="tabItemBox">
<div class="step">
  <div class="step-number">6</div>
  <div class="content">Layer wird <strong>automatisch auf die aktuelle Kartenausdehnung zugeschnitten</strong>. Um den Filter zu ändern, <strong>zoomen Sie hinein/heraus</strong> und aktualisieren Sie die Kartenausdehnung.</div>
</div>

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center'}}>

  <Video src={require('/img/map/filter/Map_extend.mp4').default} alt="Attribute Selection" style={{ maxHeight: "auto", maxWidth: "auto", objectFit: "cover"}}/>

</div> 
</TabItem>

<TabItem value="Boundary" label="Begrenzung" default className="tabItemBox">

:::info demnächst verfügbar

Diese Funktion wird derzeit entwickelt. 🧑🏻‍💻

:::
</TabItem>
</Tabs>

</TabItem>
</Tabs>

### Mehrfachausdruck-Filterung

<strong>Kombinieren Sie mehrere Filter</strong>, indem Sie die Schritte 3-7 für jeden Ausdruck wiederholen. In <code>Filter Ergebnisse</code> wählen Sie <code>Alle Filter erfüllen</code> (UND) oder <code>Mindestens einen Filter erfüllen</code> (ODER), um <strong>zu steuern, wie Filter interagieren</strong>.

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/map/filter/filter-results_de.webp').default} alt="Logic Operators" style={{ maxHeight: "300px", maxWidth: "300px", objectFit: "cover"}}/>
</div>
  
### Ausdrücke und Filter löschen

<strong>Einzelne Ausdrücke entfernen</strong>: Klicken Sie auf das <code>Weitere Optionen</code> <img src={require('/img/icons/3dots-horizontal.png').default} alt="Options" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/> Menü neben dem Ausdruck, dann klicken Sie auf <code>Löschen</code>, um <strong>den Ausdruck zu entfernen</strong>.

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/map/filter/filter_delete_clear_de.webp').default} alt="Delete expression and clear filters" style={{ maxHeight: "300px", maxWidth: "300px", objectFit: "cover"}}/>
</div>

<strong>Gesamten Filter entfernen</strong>: Klicken Sie auf <code>Filter löschen</code> am unteren Rand des Filtermenüs, um <strong>alle Filter zu entfernen</strong>.

### Als neuen Layer speichern

Klicken Sie unten im <code>Filter</code>-Menü auf <code>Als neuen Layer speichern</code>, um das **gefilterte Ergebnis als neuen Datensatz** in Ihrem Workspace zu speichern. So können Sie mit den gefilterten Daten unabhängig weiterarbeiten.

