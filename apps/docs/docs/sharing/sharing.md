---
description: "Manage team members in Settings and share datasets, projects or whole folders with a team or organization as viewer or editor, and see what each role can do."
sidebar_position: 1
---

# Teams & Members

Sharing datasets and projects allows for a more efficient workflow because **granting access to other members enables them to simultaneously edit and/or view your datasets or projects**. 

::::info
Sharing **does not duplicate** your data, only grants access to it.
::::



## Managing Teams and Members

<div class="step">
   <div class="step-number">1</div>
   <div class="content">Go to the <code>Settings</code> section.</div>
</div>

<div class="step">
   <div class="step-number">2</div>
   <div class="content">Click on a <code>Team</code> and <b>view the list of Teams</b> you are part of. Teams can represent departments or groups within your organization.</div>
</div>
<div class="step">
   <div class="step-number">3</div>
   <div class="content">Then click on the <code>Members</code> tab to <b>see the members and their roles</b>.</div>
</div>

<div class="step">
   <div class="step-number">4</div>
   <div class="content">
   If you are the <b>Owner</b> of the Organization, you can:
      <ul>
         <li>Click <code>+ New Member</code> to add a new member.</li>
         <li>Click the <code>More options</code> <img src={require('/img/icons/3dots.png').default} alt="More options" style={{ maxHeight: '20px', maxWidth: '20px', verticalAlign: 'middle'}}/> menu and then on <code>Delete</code> to remove a member</li>
      </ul>
   </div>
</div>

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <Video src={require('/img/sharing/manage_team_members.mp4').default} alt="Teams in GOAT" style={{ maxHeight: "750px", maxWidth: "750px", objectFit: "cover"}}/>
</div>
<p> </p>

:::important
When you share a dataset/project with a Team/Organization, all members will have access to it.
:::

---

## Managing access to a Dataset, Project, or Folder

Open the item's <code>More options</code> <img src={require('/img/icons/3dots.png').default} alt="More options" style={{ maxHeight: '20px', maxWidth: '20px'}}/> menu and select <code>Share</code>. The dialog has three tabs:

- **People**: share with an **individual person** from your organization. Search for them; the item lands in their `Shared with me` and stays where it lives.
- **Teams**: share with a whole **Team or Organization**. Grant its members <code>viewer</code> or <code>editor</code> access, or <code>no access</code> to withdraw it.
- **Public**: make a **dataset or project public to all GOAT users** (see [Making a dataset public](#making-a-dataset-public)).

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <Video src={require('/img/sharing/share_project.mp4').default} alt="Sharing Access in GOAT" style={{ maxHeight: "750px", maxWidth: "750px", objectFit: "cover"}}/>
</div>
<p> </p>

:::info
To withdraw access, open the <code>Share</code> dialog again and set the role back to <code>no access</code>.
:::

### Sharing a Folder

You can also share an entire **folder** at once. Sharing a folder grants access to **all datasets and projects inside it**, so you don't have to share each item individually.

<div class="step">
   <div class="step-number">1</div>
   <div class="content">Click the <code>More options</code> <img src={require('/img/icons/3dots.png').default} alt="More options" style={{ maxHeight: '20px', maxWidth: '20px'}}/> menu on a folder you own and select <code>Share</code>.</div>
</div>
<div class="step">
   <div class="step-number">2</div>
   <div class="content">Choose an <code>Organization</code> or <code>Team</code> and grant <code>viewer</code> or <code>editor</code> access as needed.</div>
</div>
<div class="step">
   <div class="step-number">3</div>
   <div class="content">To withdraw access, open the <code>Share</code> dialog again and set the role back to <code>no access</code>.</div>
</div>

:::info
A folder can be shared with **either one Organization or one Team, not both at the same time**. Items inside a shared folder inherit the folder's access, so sharing them individually is not needed.
:::

### Accessing Shared Items

You can find shared items in your workspace:

- **Projects shared with you**: <code>Workspace</code> → <code>Projects</code> → <code>Teams</code> / <code>Organizations</code>
  
- **Datasets shared with you**: <code>Workspace</code> → <code>Datasets</code> → <code>Teams</code> / <code>Organizations</code>


## Transferring ownership

You can **hand an item over to a team or organization** so that it no longer belongs to you personally. This works for datasets, projects, workflows and layouts.

<div class="step">
   <div class="step-number">1</div>
   <div class="content">Open the item's <code>More options</code> <img src={require('/img/icons/3dots.png').default} alt="More options" style={{ maxHeight: '20px', maxWidth: '20px'}}/> menu, choose <code>Share</code>, open the <code>Teams</code> tab and select <code>Transfer ownership</code>.</div>
</div>
<div class="step">
   <div class="step-number">2</div>
   <div class="content">Pick the team or organization that should own the item.</div>
</div>
<div class="step">
   <div class="step-number">3</div>
   <div class="content">For a project, <strong>tick the datasets that should move along</strong> with it. Ticked datasets become the team's too, so the project never loses them when you leave. Unticked ones <strong>stay where they are</strong>, and the team keeps seeing them through the project.</div>
</div>
<div class="step">
   <div class="step-number">4</div>
   <div class="content">Optionally enable <code>Leave a shortcut in the old location</code> so the item is still reachable from where it was, then confirm the transfer.</div>
</div>

After a transfer:

- The item **leaves your My Content** and the team (or organization) owns it. It stays with the team even if you later leave it.
- **Everyone in that team or organization gets access.** Personal shares with individual people are removed, since space membership takes over.
- Datasets still **used by projects elsewhere keep read access**, so those projects keep working.

:::info
Transferring ownership changes who the item belongs to and replaces its personal shares. If you only want to give others access without handing it over, use <code>Share</code> instead.
:::

## Making a dataset public

As a dataset's owner you can make it **public to all GOAT users**.

<div class="step">
   <div class="step-number">1</div>
   <div class="content">Open the dataset's <code>More options</code> <img src={require('/img/icons/3dots.png').default} alt="More options" style={{ maxHeight: '20px', maxWidth: '20px'}}/> menu, choose <code>Share</code>, open the <code>Public</code> tab and turn on <code>Public to all GOAT users</code>.</div>
</div>

Once public, **every signed-in GOAT user, in any organization, can view the dataset and add it to their projects**. It is not listed anywhere; people reach it through the projects and templates that include it, and editing rights do not change. When a public dataset ships with a template, it is shown with a **public badge**.

## Trash

Deleted items are not removed immediately. They **go to the Trash first**.

- Deleting an item moves it to the **Trash**, where it can be **restored for 30 days**.
- Open the Trash to <code>Restore</code> an item, or leave it: items are removed for good **30 days after deletion**.

:::info
When you transfer ownership of a folder, any trashed items inside it move along and stay in the trash.
:::

## Roles

See the table below to learn what each user can do within an Organization/Team and in a shared Dataset/Project:

<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
  <img src={require('/img/sharing/sharing_roles_table.png').default} alt="Roles Table in GOAT" style={{ maxHeight: "Auto", maxWidth: "80%", objectFit: "cover"}}/>
</div>
<p> </p>

:::info Important

Deleting a dataset from a shared project **that you own** will cause it to be *deleted for other users as well*.
**As an editor** if you delete a dataset or (layer from the) project, the *owner will still have it in their personal dataset*.

:::