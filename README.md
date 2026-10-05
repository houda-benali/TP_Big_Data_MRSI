# Big Data — Travaux pratiques

Dépôt des ressources du module Big Data.

## Organisation
- `tp1/` — Linux, Docker et préparation de Hadoop
- `tp2/` — HDFS
- `tp3/` — MapReduce
- `tp4/` — Spark
- `tp5/` — BD NOSQL
- `tp6/` — Mini-projet

## Pré-requis
- Linux
- Git
- Docker Engine
- Docker Compose v2 (`docker compose`)

Une connexion Internet est nécessaire lors de la première installation des images Docker. Une fois téléchargées, elles restent stockées localement.

## Installation git

```bash
sudo apt update
sudo apt install -y git
```

## TP1

```bash
git clone https://github.com/houdafst/TP_Big_Data_MRSI.git
cd TP_Big_Data_MRSI
```
Voir `tp1/README.md` pour le sujet complet.

## Installation Docker

```bash
chmod +x install-docker.sh
./install-docker.sh
```


