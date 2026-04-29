---
sidebar_position: 8
---

# Disjunkt

Mit diesem Werkzeug können Sie **Eingabe-Features auf jene filtern, die sich räumlich nicht mit Features eines Überlagerungs-Layers schneiden**.

## 1. Erklärung

Das **Disjunkt**-Werkzeug gibt jene Features des Eingabe-Layers zurück, die sich räumlich **nicht** mit Features des Überlagerungs-Layers schneiden: Features, die Überlagerungs-Features berühren, überlappen, enthalten oder von ihnen enthalten sind, werden ausgeschlossen; nur Features, die vollständig außerhalb der Überlagerung liegen, werden behalten.

Im Gegensatz zum Radieren werden Geometrien beim Disjunkt-Filter **nicht** verändert. Jedes Eingabe-Feature wird unverändert beibehalten oder verworfen — seine Form und Attribute bleiben erhalten.

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>

  <img src={require('/img/toolbox/geoprocessing/disjoint.png').default} alt="Disjunkt-Werkzeug" style={{ maxHeight: "400px", maxWidth: "400px", objectFit: "cover"}}/>

</div> 

## 2. Beispiel-Anwendungsfälle

- Auffinden von Punkten von Interesse außerhalb von Schutzgebieten.
- Identifizieren von Straßen, die keine Hochwasserzonen durchqueren.
- Auswahl von Gebäuden, die sich nicht innerhalb geplanter Baugebiete befinden.

## 3. Wie verwendet man das Werkzeug?

<div class="step">
  <div class="step-number">1</div>
  <div class="content">Klicken Sie auf <code>Werkzeuge</code> <img src={require('/img/icons/toolbox.png').default} alt="Options" style={{ maxHeight: "20px", maxWidth: "20px", objectFit: "cover"}}/>. </div>
</div>

<div class="step">
  <div class="step-number">2</div>
  <div class="content">Unter dem Menü <code>Geoverarbeitung</code> klicken Sie auf <code>Disjunkt</code>.</div>
</div>

<div class="step">
  <div class="step-number">3</div>
  <div class="content">Wählen Sie den <code>Eingabe-Layer</code> — den Layer, den Sie filtern möchten.</div>
</div>

<div class="step">
  <div class="step-number">4</div>
  <div class="content">Wählen Sie den <code>Überdeckungs-Layer</code> — Features des Eingabe-Layers, die sich mit einem Feature dieses Layers schneiden, werden ausgeschlossen.</div>
</div>

<div class="step">
  <div class="step-number">5</div>
  <div class="content">Klicken Sie auf <code>Ausführen</code>, um den Disjunkt-Filter auszuführen. Das Ergebnis wird zur Karte hinzugefügt und enthält nur jene Eingabe-Features, die räumlich vom Überlagerungs-Layer getrennt sind.</div>
</div>
