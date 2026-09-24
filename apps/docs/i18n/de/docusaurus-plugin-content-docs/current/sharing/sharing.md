---
sidebar_position: 6
slug: /sharing
---


# Teams & Mitglieder

Das Teilen von Datensätzen und Projekten ermöglicht einen effizienteren Arbeitsablauf, da **das Gewähren von Zugriff auf andere Mitglieder ihnen erlaubt, Ihre Datensätze oder Projekte gleichzeitig zu bearbeiten und/oder anzusehen**.

::::info
Das Teilen **dupliziert nicht** Ihre Daten, sondern gewährt nur Zugriff darauf.
::::

## Teams und Mitglieder verwalten

<div class="step">
   <div class="step-number">1</div>
   <div class="content">Gehen Sie zu <code>Einstellungen</code>.</div>
</div>
<div class="step">
   <div class="step-number">2</div>
   <div class="content">Sehen Sie die Liste der Teams, denen Sie angehören. Teams können Abteilungen oder Gruppen innerhalb Ihrer Organisation darstellen.</div>
</div>
<div class="step">
   <div class="step-number">3</div>
   <div class="content">Klicken Sie auf ein <code>Team</code> und dann auf den Tab <code>Mitglieder</code>, um die Mitglieder und ihre Rollen zu sehen.</div>
</div>
<div class="step">
   <div class="step-number">4</div>
   <div class="content">Wenn Sie <b>Besitzer</b> der Organisation sind, können Sie:
      <ul>
         <li>Auf <code>+ Neues Mitglied</code> klicken, um ein neues Mitglied hinzuzufügen.</li>
         <li>Auf das <code>Mehr Optionen</code>-Menü <img src={require('/img/icons/3dots.png').default} alt="Mehr Optionen" style={{ maxHeight: '20px', maxWidth: '20px', verticalAlign: 'middle'}}/> neben einem Mitglied klicken, um weitere Optionen wie <code>Löschen</code> zu sehen.</li>
      </ul>
   </div>
</div>

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <Video src={require('/img/sharing/manage_team_members.mp4').default} alt="Teams in GOAT" style={{ maxHeight: "750px", maxWidth: "750px", objectFit: "cover"}}/>
</div>
<p> </p>

:::important
Wenn Sie einen Datensatz/ein Projekt mit einem Team oder einer Organisation teilen, haben alle Mitglieder Zugriff darauf.
:::

---

## Zugriff auf einen Datensatz, ein Projekt oder einen Ordner verwalten

<div class="step">
   <div class="step-number">1</div>
   <div class="content">Klicken Sie bei Ihrem Datensatz oder Projekt auf <code>Mehr Optionen</code> <img src={require('/img/icons/3dots.png').default} alt="Mehr Optionen" style={{ maxHeight: '20px', maxWidth: '20px'}}/>.</div>
</div>
<div class="step">
   <div class="step-number">2</div>
   <div class="content">Wählen Sie <b>Teilen</b> und wählen Sie eine Organisation oder ein Team aus.</div>
</div>
<div class="step">
   <div class="step-number">3</div>
   <div class="content">Gewähren Sie allen Mitgliedern <b>Viewer</b>- oder <b>Editor</b>-Zugriff nach Bedarf.</div>
</div>

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <Video src={require('/img/sharing/share_project.mp4').default} alt="Teilen-Zugriff in GOAT" style={{ maxHeight: "750px", maxWidth: "750px", objectFit: "cover"}}/>
</div>
<p> </p>

<div class="step">
   <div class="step-number">4</div>
   <div class="content">Wenn Sie den Zugriff entziehen möchten, klicken Sie auf <code>Mehr Optionen</code> <img src={require('/img/icons/3dots.png').default} alt="Mehr Optionen" style={{ maxHeight: '20px', maxWidth: '20px'}}/> und wählen Sie <code>Kein Zugriff</code>.</div>
</div>

### Einen Ordner teilen

Sie können auch einen ganzen **Ordner** auf einmal teilen. Das Teilen eines Ordners gewährt Zugriff auf **alle darin enthaltenen Datensätze und Projekte**, sodass Sie nicht jedes Element einzeln teilen müssen.

<div class="step">
   <div class="step-number">1</div>
   <div class="content">Klicken Sie bei einem Ordner, den Sie besitzen, auf <code>Mehr Optionen</code> <img src={require('/img/icons/3dots.png').default} alt="Mehr Optionen" style={{ maxHeight: '20px', maxWidth: '20px'}}/> und wählen Sie <code>Teilen</code>.</div>
</div>
<div class="step">
   <div class="step-number">2</div>
   <div class="content">Wählen Sie eine <code>Organisation</code> oder ein <code>Team</code> und gewähren Sie <code>Viewer</code>- oder <code>Editor</code>-Zugriff nach Bedarf.</div>
</div>
<div class="step">
   <div class="step-number">3</div>
   <div class="content">Um den Zugriff zu entziehen, öffnen Sie den <code>Teilen</code>-Dialog erneut und setzen Sie die Rolle zurück auf <code>Kein Zugriff</code>.</div>
</div>

:::info
Ein Ordner kann **entweder mit einer Organisation oder mit einem Team geteilt werden, nicht mit beiden gleichzeitig**. Elemente in einem geteilten Ordner erben den Zugriff des Ordners, sodass ein individuelles Teilen nicht erforderlich ist.
:::

### Geteilte Elemente aufrufen

Sie finden geteilte Elemente in Ihrem Workspace:

- Projekte, die mit Ihnen geteilt wurden: <code>Workspace</code> → <code>Projects</code> → <code>Teams</code> / <code>Organizations</code>
  
- Datensätze, die mit Ihnen geteilt wurden: <code>Workspace</code> → <code>Datasets</code> → <code>Teams</code> / <code>Organizations</code>

## Rollen

Siehe die Tabelle unten, um zu erfahren, was jeder Benutzer innerhalb einer Organisation/eines Teams und in einem geteilten Datensatz/Projekt tun kann:

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/sharing/sharing_roles_table.png').default} alt="Rollen-Tabelle in GOAT" style={{ maxHeight: "auto", maxWidth: "80%", objectFit: "cover"}}/>
</div>
<p> </p>

:::info Wichtig

Das Löschen eines Datensatzes aus einem geteilten Projekt, **das Sie besitzen**, führt dazu, dass es *auch für andere Benutzer gelöscht wird*.

**Als Editor**: Wenn Sie einen Datensatz oder eine Ebene aus dem Projekt löschen, bleibt dieser *für den Besitzer weiterhin im persönlichen Datensatz erhalten*.

:::
