---
description: "Verwalten Sie Team-Mitglieder in den Einstellungen und teilen Sie Datensätze, Projekte oder Ordner mit einem Team oder der Organisation als Viewer oder Editor."
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
   <div class="content">
   Wenn Sie <b>Besitzer</b> der Organisation sind, können Sie:
      <ul>
         <li>Auf <code>+ Neues Mitglied</code> klicken, um ein neues Mitglied hinzuzufügen.</li>
         <li>Auf das <code>Mehr Optionen</code>-Menü <img src={require('/img/icons/3dots.png').default} alt="Mehr Optionen" style={{ maxHeight: '20px', maxWidth: '20px', verticalAlign: 'middle'}}/> neben einem Mitglied klicken, um weitere Optionen wie <code>Löschen</code> zu sehen.</li>
      </ul>
   </div>
</div>

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/sharing/manage_team_members_de.webp').default} alt="Mitglieder in den Einstellungen verwalten" style={{ maxHeight: "auto", maxWidth: "100%", objectFit: "contain"}}/>
</div>
<p> </p>

:::important
Wenn Sie einen Datensatz/ein Projekt mit einem Team oder einer Organisation teilen, haben alle Mitglieder Zugriff darauf.
:::

---

## Zugriff auf einen Datensatz, ein Projekt oder einen Ordner verwalten

Öffnen Sie das <code>Mehr Optionen</code>-Menü <img src={require('/img/icons/3dots.png').default} alt="Mehr Optionen" style={{ maxHeight: '20px', maxWidth: '20px'}}/> des Inhalts und wählen Sie <code>Teilen</code>. Der Dialog hat drei Tabs:

- **Personen**: mit einer **einzelnen Person** aus Ihrer Organisation teilen. Suchen Sie sie; der Inhalt erscheint bei ihr unter `Mit mir geteilt` und bleibt, wo er liegt.
- **Teams**: mit einem ganzen **Team oder einer Organisation** teilen. Gewähren Sie den Mitgliedern <code>Viewer</code>- oder <code>Editor</code>-Zugriff, oder <code>Kein Zugriff</code>, um ihn zu entziehen.
- **Öffentlich**: einen **Datensatz** für alle GOAT-Nutzer öffentlich machen; bei einem **Projekt** eine öffentliche Momentaufnahme veröffentlichen, die jeder ohne GOAT-Konto öffnen kann. Beides wird unter [Öffentliches Teilen](./public.md) behandelt.

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/sharing/share_dialog_tabs_de.webp').default} alt="Teilen öffnen und die Tabs Personen, Teams und Öffentlich" style={{ maxHeight: "auto", maxWidth: "100%", objectFit: "contain"}}/>
</div>
<p> </p>

:::info
Um den Zugriff zu entziehen, öffnen Sie den <code>Teilen</code>-Dialog erneut und setzen Sie die Rolle zurück auf <code>Kein Zugriff</code>.
:::

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

## Rechte übertragen

Sie können einen Inhalt **an ein Team oder eine Organisation übergeben**, sodass er nicht mehr Ihnen persönlich gehört. Dies funktioniert für Datensätze, Projekte, Workflows und Layouts.

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/sharing/transfer_ownership_de.webp').default} alt="Rechte übertragen im Tab Teams des Teilen-Dialogs" style={{ maxHeight: "480px", maxWidth: "480px", objectFit: "contain"}}/>
</div>
<p> </p>

<div class="step">
   <div class="step-number">1</div>
   <div class="content">Öffnen Sie das <code>Weitere Optionen</code>-Menü <img src={require('/img/icons/3dots.png').default} alt="Weitere Optionen" style={{ maxHeight: '20px', maxWidth: '20px'}}/> des Inhalts, wählen Sie <code>Teilen</code>, öffnen Sie den Tab <code>Teams</code> und wählen Sie <code>Rechte übertragen</code>.</div>
</div>
<div class="step">
   <div class="step-number">2</div>
   <div class="content">Wählen Sie das Team oder die Organisation, das/die den Inhalt besitzen soll.</div>
</div>
<div class="step">
   <div class="step-number">3</div>
   <div class="content">Bei einem Projekt <strong>kreuzen Sie die Datensätze an, die mitwandern sollen</strong>. Angekreuzte Datensätze gehören dann ebenfalls dem Team, sodass das Projekt sie nicht verliert, wenn Sie es verlassen. Nicht angekreuzte <strong>bleiben, wo sie sind</strong>, und das Team sieht sie weiterhin über das Projekt.</div>
</div>
<div class="step">
   <div class="step-number">4</div>
   <div class="content">Aktivieren Sie optional <code>Verknüpfung am alten Ort hinterlassen</code>, damit der Inhalt weiterhin von seinem bisherigen Ort erreichbar ist, und bestätigen Sie die Übertragung.</div>
</div>

Nach einer Übertragung:

- Der Inhalt verlässt **„Meine Inhalte"** und gehört dem Team (oder der Organisation). Er bleibt beim Team, auch wenn Sie es später verlassen.
- **Alle im Team oder in der Organisation erhalten Zugriff.** Persönliche Freigaben für einzelne Personen werden entfernt, da die Mitgliedschaft im Bereich übernimmt.
- Datensätze, die weiterhin **von Projekten anderswo genutzt werden, behalten Lesezugriff**, sodass diese Projekte weiter funktionieren.

:::info
Das Übertragen der Rechte ändert, wem der Inhalt gehört, und ersetzt dessen persönliche Freigaben. Wenn Sie anderen nur Zugriff geben möchten, ohne ihn zu übergeben, verwenden Sie stattdessen <code>Teilen</code>.
:::

## Papierkorb

Gelöschte Inhalte werden nicht sofort entfernt. Sie **wandern zuerst in den Papierkorb**.

Den Papierkorb finden Sie unter <code>Inhalt</code>, im Panel <code>Bereiche</code> auf der linken Seite.

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/sharing/trash_de.webp').default} alt="Papierkorb unten im Panel Bereiche unter Inhalt" style={{ maxHeight: "auto", maxWidth: "100%", objectFit: "contain"}}/>
</div>
<p> </p>

- Beim Löschen wird ein Inhalt in den **Papierkorb** verschoben, wo er **30 Tage lang wiederhergestellt** werden kann.
- Öffnen Sie den Papierkorb, um einen Inhalt <code>Wiederherstellen</code> zu lassen, oder belassen Sie ihn dort: Inhalte werden **30 Tage nach dem Löschen** endgültig entfernt.

:::info
Wenn Sie die Rechte an einem Ordner übertragen, wandern darin enthaltene, gelöschte Inhalte mit und bleiben im Papierkorb.
:::

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
