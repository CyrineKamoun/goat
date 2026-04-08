---
sidebar_position: 1
---

# Workflow-Benutzeroberfläche

**Workflows** in GOAT bieten ein mächtiges visuelles Automatisierungssystem zur Erstellung anspruchsvoller räumlicher Analysepipelines. Anstatt einzelne Werkzeuge nacheinander auszuführen, können Sie mehrere Analyseschritte mit einer Drag-and-Drop-Leinwand verbinden und dabei eine automatisierte Datenverarbeitung erstellen, die wiederholende manuelle Arbeit eliminiert.

Die Workflows können bei verschiedenen Datensätzen und Szenarien wiederverwendet werden. Jeder Workflow besteht aus verschiedenen Arten von [Knoten](../further_reading/glossary.md#knoten), die durch [Kanten](../further_reading/glossary.md#kanten) verbunden sind, und ermöglicht Ihnen:

- **Automatisierung komplexer analytischer Pipelines**: Verketten Sie mehrere Werkzeuge, bei denen die Ausgabe einer Analyse automatisch in die nächste eingespeist wird
- **Erstellen von Multi-Source-Daten-Workflows**: Bauen Sie anspruchsvolle Analyseprozesse auf, die mehrere Datensätze und Verarbeitungsschritte integrieren
- **Dokumentieren automatisierter Prozesse**: Fügen Sie Textanmerkungen hinzu, um Methodik und analytische Entscheidungen zu erklären
- **Ausführung mit flexibler Automatisierung**: Führen Sie einzelne Knoten aus, führen Sie Workflow-Segmente aus oder automatisieren Sie ganze Pipelines
- **Erstellen wiederverwendbarer Automatisierungsvorlagen**: Speichern Sie Workflows innerhalb von Projekten für die Wiederholbarkeit bei verschiedenen Szenarien
- **Nutzen erweiterter Automatisierungsfeatures**: Verwenden Sie [Workflow-Variablen](variables.md) und [benutzerdefinierte SQL](custom_sql.md) für anspruchsvolle parametrisierte Analysen

Die visuelle Leinwand-Benutzeroberfläche macht komplexe räumliche Analyseautomatisierung für Benutzer aller technischen Ebenen zugänglich und bewahrt dabei die vollständige Dokumentation des analytischen Prozesses für Reproduzierbarkeit und Zusammenarbeit.

## 1. Benutzeroberflächen-Komponenten

Die Workflow-Benutzeroberfläche besteht aus zwei Hauptpanels und der Workflow-Leinwand und bietet einen intuitiven Arbeitsbereich für die visuelle Workflow-Konstruktion.

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/workflows/workflows_interface.webp').default} alt="Kartenoberfläche Übersicht" style={{ maxHeight: "auto", maxWidth: "auto", objectFit: "cover"}}/>
</div> 

### Workflow-Management und Projekt-Ebenen Panel
Dieses Panel befindet sich links und ist in zwei Bereiche unterteilt:

#### Workflow-Management

- **Workflow hinzufügen**: Klicken Sie auf <code>+ Workflow hinzufügen</code>, um neue analytische Pipelines zu erstellen

- **Workflow-Liste**: Verwalten Sie vorhandene Workflows mit Optionen zum Umbenennen, Duplizieren und Löschen

#### Projekt-Ebenen

- **Ebenenbaum**: Schreibgeschützte Anzeige der Datenebenen des Projekts. Sie können sie per Drag-and-Drop auf die Leinwand ziehen, um den Workflow zu erstellen.

- **Ebene hinzufügen**: Fügen Sie neue Ebenen zum Projekt hinzu, um sie im Workflow- und Kartenmodus zu verwenden.

### Workflow-Leinwand

#### Leinwand-Arbeitsbereich

Der Leinwand-Arbeitsbereich ist der Ort, wo Sie Knoten per Drag-and-Drop bewegen, zoomen, schwenken und Elemente auswählen können. Er enthält mehrere Steuerungsbereiche:

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/workflows/workflows_canvas-bars.webp').default} alt="Kartenoberfläche Übersicht" style={{ maxHeight: "auto", maxWidth: "auto", objectFit: "cover"}}/>
</div> 

**Leinwand-Ansichtsleiste**: Befindet sich in der unteren linken Ecke der Leinwand:
-  <img src={require('/img/icons/plus.png').default} alt="Hineinzoomen" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/> <code>Hineinzoomen</code>: <strong>Vergrößert</strong> die Leinwand-Vergrößerung für detaillierte Arbeit
-  <img src={require('/img/icons/minus.png').default} alt="Herauszoomen" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/> <code>Herauszoomen</code>: <strong>Verringert</strong> die Leinwand-Vergrößerung, um mehr vom Workflow zu sehen
-  <img src={require('/img/icons/fit-view.png').default} alt="Ansicht anpassen" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/> <code>Ansicht anpassen</code>: <strong>Passt die Ansicht an</strong>, um den gesamten Workflow auf einmal anzuzeigen
-  <img src={require('/img/icons/lock.png').default} alt="Ansicht sperren" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/> <code>Ansicht sperren</code>: <strong>Verhindert versehentliches Schwenken und Zoomen</strong> der Leinwand
  
**Symbolleisten-Steuerung**: Befindet sich am mittleren unteren Rand der Leinwand:
- <img src={require('/img/icons/cursor.png').default} alt="Auswählen" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/> <code>Auswählen</code>: <strong>Standard-Cursor</strong> zum Auswählen und Bewegen von Knoten
- <img src={require('/img/icons/text-card.png').default} alt="Textkarte" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/> <code>Textkarte</code>: <strong>Fügen Sie Textanmerkungen hinzu</strong> zur Dokumentation von Workflow-Schritten
- <img src={require('/img/icons/redo.png').default} alt="Wiederholen" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/> <code>Wiederholen</code>: <strong>Stellt die letzte rückgängig gemachte Aktion wieder her</strong>
- <img src={require('/img/icons/undo.png').default} alt="Rückgängig" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/> <code>Rückgängig</code>: <strong>Macht die letzte Aktion rückgängig</strong>
- <img src={require('/img/icons/variables.png').default} alt="Variablen" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/> <code>Variablen</code>: <strong>Erstellen und verwalten Sie</strong> [Workflow-Variablen](variables.md) für wiederverwendbare Parameter
- <img src={require('/img/icons/play.png').default} alt="Ausführen" style={{ maxHeight: "40px", maxWidth: "40px", objectFit: "cover"}}/> <code>Ausführen</code>: <strong>Führt den gesamten Workflow aus</strong>

**Minimap**: Befindet sich in der unteren rechten Ecke der Leinwand und bietet einen Übersichtsnavigator für komplexe Workflows.

**Datenansicht-Steuerung**: Befindet sich am unteren Rand der Leinwand:
- <code>Tabelle anzeigen</code>: Zeigt Attributdaten für ausgewählte Knotenergebnisse an
- <code>Karte anzeigen</code>: Visualisiert räumliche Daten für ausgewählte Knotenausgaben

### Werkzeuge und Konfigurations-Panel
Das rechte Panel ändert sich je nachdem, ob ein Knoten ausgewählt ist oder nicht. Wenn kein Knoten ausgewählt ist, wird das **Werkzeuge und Verlauf Panel** sichtbar. Wenn ein Werkzeug-Knoten ausgewählt ist, erscheint das **Konfigurations-Panel**, und wenn ein Datensatz-Knoten ausgewählt ist, erscheint das **Datensatz-Panel**.

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/workflows/workflow_right-panel.webp').default} alt="Kartenoberfläche Übersicht" style={{ maxHeight: "auto", maxWidth: "auto", objectFit: "cover"}}/>
</div> 

#### Werkzeuge und Verlauf Panel

**Werkzeuge Tab**

Dieser Tab enthält kategorisierte Werkzeuge, die für die Workflow-Konstruktion verfügbar sind, ähnlich der Karten-Modus-Toolbox. Ziehen Sie Werkzeuge per Drag-and-Drop auf die Leinwand, um sie zu Ihrem Workflow hinzuzufügen. Die Werkzeuge sind in die folgenden Kategorien unterteilt:

- **Import**
  - <code>+ Datensatz hinzufügen</code>: Erstellt Datensatz-Knoten
  - <code>Datensatz speichern</code>: Speichert Workflow-Ergebnisse als permanente Datensätze

- **Erreichbarkeitsindikatoren**
  - Alle Werkzeuge, die im Abschnitt [Erreichbarkeitsindikatoren](../category/accessibility-indicators) der Toolbox verfügbar sind

- **Geoanalyse**
  - Alle Werkzeuge, die im Abschnitt [Geoanalyse](../category/geoanalysis) der Toolbox verfügbar sind

- **Geoprozessierung**
  - Alle Werkzeuge, die im Abschnitt [Geoprozessierung](../category/geoprocessing) der Toolbox verfügbar sind
  
- **Datenmanagement**
  - [Verknüpfen](../toolbox/data_management/join.md) und andere Datenmanipulations-Werkzeuge
  - [Benutzerdefinierte SQL](custom_sql.md): Erweiterte Datenverarbeitung mit SQL-Abfragen

**Verlauf Tab**
Hier können Sie Folgendes sehen:
- **Ausführungsprotokoll**: Vorherige Workflow-Läufe mit Zeitstempeln und Status
- **Ausführungsdetails**: Dauer, Erfolg-/Fehlerstatus und Fehlermeldungen
- **Ergebniszugriff**: Links zu vorherigen Workflow-Ausgaben

#### Konfigurations-Panel (Werkzeug-Knoten ausgewählt)
Wenn ein Werkzeug-Knoten ausgewählt ist, zeigt das rechte Panel das **Werkzeug-Konfigurations**-Panel an. Konfigurieren Sie alle werkzeugspezifischen Parameter für die ausgewählte Analyse. Sie können auch [Workflow-Variablen](variables.md) in Parameterfeldern für dynamische Werte verwenden.

#### Datensatz Panel (Datensatz-Knoten ausgewählt)
Wenn ein **Datensatz-Knoten** ausgewählt ist, erscheint das Datensatz-Panel mit zwei verfügbaren Tabs:

**Quelle Tab**: Zeigen Sie Metadaten aus der Datenquelle an und greifen Sie auf Tabellen- und Kartenansichten zu. Sie können auch den dem Knoten zugewiesenen Datensatz von diesem Tab aus ändern.

**Filter Tab**: Wenden Sie spezifische Filter für den Workflow an, ohne die ursprüngliche Ebene zu beeinträchtigen.

## 2. Beispielhafte Anwendungsfälle

- **Erreichbarkeitsanalyse-Pipeline**: Erstellen Sie [Einzugsgebiete](../toolbox/accessibility_indicators/catchments.md), überschneiden Sie mit Bevölkerungsdaten, berechnen Sie Erreichbarkeitsindikatoren und exportieren Sie Ergebnisse
- **Standorteignungsbewertung**: [Puffern](../toolbox/geoprocessing/buffer.md) Sie Beschränkungen, führen Sie räumliche Überlagerungen durch, wenden Sie Gewichtungsfaktoren an und bewerten Sie geeignete Standorte
- **Multi-Source-Datenintegration**: [Verknüpfen](../toolbox/data_management/join.md) Sie mehrere Datensätze, wenden Sie räumliche Filter an, aggregieren Sie Statistiken und erstellen Sie umfassende analytische Ausgaben
- **Qualitätsbewertungs-Workflow**: Validieren Sie Datenqualität, prüfen Sie räumliche Beziehungen, generieren Sie Validierungsberichte mit [benutzerdefinierter SQL](custom_sql.md)
- **Vergleichende Szenarioanalyse**: Verwenden Sie [Workflow-Variablen](variables.md), um identische Analysen mit unterschiedlichen Parametern oder Datensätzen durchzuführen

## 3. Wie man die Workflow-Benutzeroberfläche verwendet

:::tip Erste Schritte
Beginnen Sie mit einfachen 2-3 Knoten-Workflows, um die Benutzeroberfläche zu verstehen, und bauen Sie dann schrittweise komplexere analytische Pipelines auf, wenn Sie sich mit dem System vertraut gemacht haben.
:::

### Erstellen Ihres ersten Workflows

<div class="step">
  <div class="step-number">1</div>
  <div class="content"><strong>Navigieren Sie zu Workflows</strong>: Klicken Sie auf den Tab <code>Workflows</code> in GOATs Hauptnavigation, um auf die Workflow-Benutzeroberfläche zuzugreifen.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content"><strong>Erstellen Sie einen neuen Workflow</strong>: Klicken Sie auf <code>+ Workflow hinzufügen</code> im linken Panel, um einen neuen [Workflow](../further_reading/glossary.md#workflow) zu erstellen.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content"><strong>Benennen Sie Ihren Workflow</strong>: Geben Sie einen aussagekräftigen Namen ein, der Ihr analytisches Ziel widerspiegelt (z.B. "Städtische Erreichbarkeitsanalyse", "Umweltauswirkungsbewertung").</div>
</div>

<div class="step">
  <div class="step-number">4</div>
  <div class="content"><strong>Beschreibung hinzufügen</strong>: Geben Sie eine optionale Beschreibung an, die den Zweck und die Methodik des Workflows dokumentiert.</div>
</div>

### Erstellen Ihres Workflows

<div class="step">
  <div class="step-number">1</div>
  <div class="content"><strong>Datenquellen hinzufügen</strong>: Fügen Sie Daten zu Ihrem Workflow hinzu, indem Sie entweder <code>+ Datensatz hinzufügen</code> vom Werkzeuge Tab des rechten Panels auf die Leinwand ziehen oder indem Sie Ebenen direkt vom Projekt-Ebenen Panel links ziehen. Konfigurieren Sie den Datensatz-Knoten so, dass er auf Ihre Eingangsdaten-Ebenen verweist.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content"><strong>Analysewerkzeuge hinzufügen</strong>: Durchsuchen Sie die kategorisierten Werkzeugabschnitte und ziehen Sie die erforderlichen Analysewerkzeuge auf Ihre Leinwand. Werkzeuge sind nach Funktionen organisiert (Erreichbarkeit, Geoprozessierung, usw.).</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content"><strong>Workflow-Elemente verbinden</strong>: Erstellen Sie Kanten, indem Sie von Ausgabehandles zu Eingabehandles ziehen. GOAT validiert automatisch die Geometriekompatibilität.</div>
</div>

<div class="step">
  <div class="step-number">4</div>
  <div class="content"><strong>Parameter konfigurieren</strong>: Klicken Sie auf jeden Werkzeug-Knoten, um Analyseparameter zu setzen. Verwenden Sie Workflow-Variablen für flexible, wiederverwendbare Konfigurationen.</div>
</div>

<div class="step">
  <div class="step-number">5</div>
  <div class="content"><strong>Dokumentieren Sie Ihren Prozess</strong>: Fügen Sie Textanmerkungen hinzu, um Ihre Methodik und analytischen Entscheidungen zu erklären.</div>
</div>

### Ausführung und Verwaltung von Workflows

<div class="step">
  <div class="step-number">1</div>
  <div class="content"><strong>Workflow ausführen</strong>: Verwenden Sie die Option <code>Workflow ausführen</code>, um den gesamten Workflow von Anfang bis Ende auszuführen.</div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content"><strong>Ausführung überwachen</strong>: Beobachten Sie die Fortschrittsindikatoren und prüfen Sie den Job-Status in der Hauptbenutzeroberfläche für Ausführungsupdates.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content"><strong>Ergebnisse überprüfen</strong>: Verwenden Sie die Schaltflächen <code>Tabelle anzeigen</code> und <code>Karte anzeigen</code>, um die endgültigen Ergebnisse zu inspizieren, sobald der Workflow abgeschlossen ist.</div>
</div>

<div class="step">
  <div class="step-number">4</div>
  <div class="content"><strong>Ergebnisse speichern</strong>: Fügen Sie Export-Knoten hinzu und konfigurieren Sie sie, um wichtige Ergebnisse als permanente Datensätze in Ihrem Projekt zu speichern.</div>
</div>

### Ergebnisse

Die erfolgreiche Nutzung der Workflow-Benutzeroberfläche bietet:

- **Reproduzierbare Analyse**: Dokumentierte analytische Prozesse, die mit verschiedenen Daten oder Parametern erneut ausgeführt werden können
- **Effizienter Workflow**: Rationalisierte mehrstufige Analyseausführung mit automatischem Abhängigkeitsmanagement  
- **Qualitätskontrolle**: Validierungsmöglichkeiten bei jedem Schritt komplexer analytischer Pipelines
- **Kollaborative Dokumentation**: Visuelle Darstellung der Methodik für Teamfreigabe und Wissenstransfer
- **Erweiterte Fähigkeiten**: Zugriff auf spezialisierte Werkzeuge wie [benutzerdefinierte SQL](custom_sql.md) und [Workflow-Variablen](variables.md) für anspruchsvolle Analysen

:::info Auto-Speicher-Feature
Workflows speichern Änderungen automatisch, während Sie sie erstellen. Das System bewahrt alle Konfigurationen, Verbindungen und Ausführungszustände auf.
:::