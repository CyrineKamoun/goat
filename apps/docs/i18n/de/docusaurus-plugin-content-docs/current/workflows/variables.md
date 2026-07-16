# Variablen

**Workflow-Variablen** ermöglichen es Ihnen, wiederverwendbare Werte zu definieren, die zur Laufzeit gesetzt werden können, ohne den Workflow zu bearbeiten. Nutzen Sie sie, um Ihre Analyse flexibel und teilbar zu machen — Mitarbeitende können denselben Workflow mit anderen Parametern ausführen, ohne die Workflow-Struktur zu ändern.

Variablen verwenden die Syntax `{{@variable_name}}` und können in die meisten Werkzeugparameter eingefügt werden.

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/workflows/variables_de.webp').default} alt="Workflow-Variablen" style={{ maxHeight: "auto", maxWidth: "auto", objectFit: "cover"}}/>
</div>

## Variablentypen

| Typ | Verwendung |
|---|---|
| **Text** | Textwerte — Namen, Beschriftungen, Filterkriterien |
| **Zahl** | Numerische Werte — Entfernungen, Schwellenwerte, Zählungen |

## Variablen erstellen

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Klicken Sie auf das <code>{"{}"}</code>-Symbol <strong>Variablen</strong> in der Werkzeugleiste am unteren Rand der Leinwand, um den Variablen-Dialog zu öffnen.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Klicken Sie auf <strong>Variable hinzufügen</strong>.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Geben Sie einen <strong>Namen</strong> ein. Namen müssen mit einem Buchstaben oder Unterstrich beginnen und dürfen nur Buchstaben, Ziffern und Unterstriche enthalten — keine Leerzeichen (z. B. <code>puffer_abstand</code>, nicht <code>puffer abstand</code>).</div>
</div>

<div class="step">
  <div class="step-number">4</div>
  <div class="content">Wählen Sie einen <strong>Typ</strong>: <code>Text</code> oder <code>Zahl</code>.</div>
</div>

<div class="step">
  <div class="step-number">5</div>
  <div class="content">Geben Sie optional einen <strong>Standardwert</strong> ein.</div>
</div>

<div class="step">
  <div class="step-number">6</div>
  <div class="content">Klicken Sie auf <strong>Fertig</strong>, um zu speichern. Um eine Variable zu löschen, klicken Sie auf das Löschen-Symbol daneben.</div>
</div>

## Variablen in Werkzeugparametern verwenden

Klicken Sie auf das <code>{"{}"}</code>-Symbol in einem kompatiblen Parameterfeld und wählen Sie die gewünschte Variable aus dem Menü. Nach dem Einfügen zeigt das Feld den Variablenverweis (z. B. <code>{"{{@variable_name}}"}</code>) grün hervorgehoben an — das signalisiert, dass das Feld durch eine Variable gesteuert wird.

:::tip
Verwenden Sie beschreibende Namen, die den Zweck des Parameters klar angeben: `puffer_abstand` statt `abstand`, `poi_typ` statt `typ`.
:::

## Workflows mit Variablen aus der Kartenansicht ausführen

Workflows mit Variablen können direkt aus der Kartenansicht ausgeführt werden, ohne den Workflow-Editor zu öffnen.

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Öffnen Sie die <code>Toolbox</code> und klicken Sie auf den Tab <strong>Workflows</strong>.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Klicken Sie auf einen Workflow aus der Liste, um ihn zu öffnen.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Ein Abschnitt <strong>Variablen</strong> zeigt alle im Workflow definierten Variablen. Geben Sie die gewünschten Werte für diesen Durchlauf ein.</div>
</div>

<div class="step">
  <div class="step-number">4</div>
  <div class="content">Klicken Sie auf <strong>Ausführen</strong>. Der Workflow wird mit den eingegebenen Werten ausgeführt. Klicken Sie auf <strong>Zurücksetzen</strong>, um die Standardwerte wiederherzustellen.</div>
</div>

:::info
Die hier eingegebenen Werte gelten nur für diesen Durchlauf. Beim nächsten Öffnen des Workflows werden die Standardwerte wiederhergestellt.
:::
