---
sidebar_position: 3
sidebar_label: Kubernetes (Helm)
---

# Kubernetes (Helm)

Für Kubernetes-Cluster gibt es ein Helm-Chart für GOAT, veröffentlicht unter `oci://ghcr.io/plan4better/charts/goat`. Diese Seite fasst zusammen, was das Chart enthält und was Sie bereitstellen müssen. Die [README des Charts](https://github.com/plan4better/charts/tree/main/charts/goat) ist die vollständige Referenz aller Values.

:::tip Ein Server? Nutzen Sie Docker Compose
Für einen einzelnen Server ist das [Docker-Compose-Paket](./docker_compose/installation.md) der empfohlene Weg. Es enthält den Anmeldeserver, den Objektspeicher und HTTPS, die das Helm-Chart Ihnen überlässt.
:::

Diese Seite beschreibt die Chart-Version **0.5.1**, die standardmäßig GOAT **v3.0.3** installiert.

## Installation {#install}

```bash
helm install goat oci://ghcr.io/plan4better/charts/goat \
  --version 0.5.1 \
  --namespace goat --create-namespace \
  --values your-values.yaml \
  --wait --timeout 25m
```

Eine erste Installation lädt mehrere große Images, daher der lange Timeout. Ein frischer Cluster benötigt nur diesen einen Befehl.

## Was das Chart enthält {#included}

| Komponente | Vorgabe |
|---|---|
| core, web, geoapi, processes, catalog | an |
| Windmill-Server und der Standard-Worker | an |
| Windmill-Worker `tools`, `workflows` und `print` | **aus** |
| PostgreSQL über CloudNativePG (Operator und Cluster) | an, optionales Sub-Chart |
| Redis | an, optionales Sub-Chart |
| Ein gemeinsames Daten-Volume (`data`, 200 Gi, `ReadWriteOnce`) | an |
| Caddy für eigene Domains | aus |

Das Chart enthält **nicht**:

- **Objektspeicher.** GOAT benötigt einen externen S3-kompatiblen Speicher.
- **Keycloak.** Wenn sich Benutzer anmelden sollen, benötigen Sie ein externes Keycloak.

Statt der mitgelieferten Instanzen können Sie das Chart auch auf ein bestehendes PostgreSQL oder Redis richten; die README des Charts listet das SQL auf, das Ihr PostgreSQL benötigt.

## Was Sie einstellen müssen {#required-values}

### Öffentliche URLs {#public-urls}

Der Browser ruft core, geoapi, processes und catalog direkt auf, deshalb benötigt die Web-App deren öffentliche Adressen. Geben Sie jedem Dienst ein Ingress mit Host (`<service>.ingress.*`), dann werden die URLs daraus abgeleitet, oder setzen Sie sie ausdrücklich in `web.publicUrls.api`, `.geoapi`, `.processes` und `.catalog`.

:::warning
Ohne diese URLs zeigt die Web-App eine leere Seite.
:::

### Objektspeicher {#storage}

Das Chart bringt Platzhalter für S3 mit. Ersetzen Sie sie durch Endpunkt, Region, Bucket und Zugangsdaten Ihres Speichers in der Konfiguration von core, geoapi und processes (`<service>.config.S3_*`, die Schlüssel über `<service>.extraEnv`), und ebenso für die Analyse-Worker (siehe unten).

### Anmeldung {#auth}

Die Anmeldung ist standardmäßig **aus** (`global.auth.enabled: false`): Alle Dienste handeln dann als ein Standardbenutzer, stellen Sie eine solche Installation also hinter Ihre eigene Zugriffskontrolle. Um die Anmeldung einzuschalten, setzen Sie `global.auth.enabled: true` und `global.auth.existingSecret` auf ein Secret mit den Schlüsseln `server-url`, `realm`, `client-id`, `client-secret` und `nextauth-secret`. Ein Secret versorgt alle fünf Dienste.

### Analyse-Worker {#workers}

Der Standard-Worker von Windmill übernimmt nur kleine Jobs. **Analyse-Werkzeuge, Datensatz-Importe und Workflows benötigen die Worker `tools` und `workflows`, der PDF-Druck benötigt den Worker `print`.** Alle drei sind standardmäßig aus. So nutzen Sie sie:

- Schalten Sie sie mit `windmill.workers.tools.enabled`, `windmill.workers.workflows.enabled` und `windmill.workers.print.enabled` ein.
- Die Worker `tools` und `workflows` sind an Nodes mit dem Label `node.kubernetes.io/server-usage: geodata` und der Toleration `geodata=true:NoSchedule` gebunden. Bereiten Sie solche Nodes vor oder überschreiben Sie `nodeSelector` und `tolerations` für diese Worker. Andernfalls bleiben ihre Pods im Zustand `Pending`, und `helm install --wait` läuft in den Timeout.
- Geben Sie den Workern die Einstellungen für die GOAT-Datenbank und S3 über ihre `config` und `extraEnv`. Windmill reicht nur die Variablen an die Jobs weiter, die in `WHITELIST_ENVS` stehen; das Chart bringt eine Liste mit, die GOATs Werkzeuge abdeckt. Ergänzen Sie sie, statt sie zu ersetzen.
- Schalten Sie `windmill.scriptSync.enabled` ein, um GOATs Analyse-Werkzeuge und Aufgaben nach der Installation in Windmill zu registrieren. Die Einstellung ist standardmäßig aus.

### Speicher für mehrere Nodes {#rwx}

Das gemeinsame Daten-Volume verwendet standardmäßig `ReadWriteOnce`, was auf einem einzelnen Node funktioniert. In einem Cluster mit mehreren Nodes setzen Sie `data.accessMode: ReadWriteMany` mit einer RWX-fähigen Storage-Class (`data.storageClassName`) oder stellen mit `data.existingClaim` Ihren eigenen Claim bereit.

## Weiterführende Informationen {#further-reading}

Die [README des Charts](https://github.com/plan4better/charts/tree/main/charts/goat) behandelt die Details: die Installation auf einem frischen Cluster, die Einrichtung eines externen PostgreSQL, die Rangfolge der Authentifizierungseinstellungen, das gemeinsame Daten-Volume, die Region, in der neue Projekte öffnen (`DEFAULT_PROJECT_VIEW_STATE`), Schema-Migrationen und die Upgrade-Hinweise zwischen den Chart-Versionen.
