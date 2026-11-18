# TP3 --- MapReduce : Premier traitement distribué

**FST-SBZ --- A.U. 2026-2027 --- 2ème MRSI**

**Dr. -Ing. Houda BENALI**

**Prérequis : TP1 et TP2 validés**

## Objectifs

À la fin du TP, vous devez savoir : 
- expliquer Map → Shuffle/Sort → Reduce ; 
- manipuler les paires clé/valeur ; 
- lancer un job Hadoop Streaming avec Python ; 
- lire et interpréter les fichiers de sortie `part-*` ; 
- modifier le nombre de Reducers ; 
- développer et exécuter une application MapReduce en Java ; 
- configurer et comprendre le rôle du Combiner ; 
- interpréter les compteurs d'un Job ; 
- en extension, développer un Partitioner personnalisé.

Le TP3 réutilise le cluster Hadoop mis en place au TP1 ainsi que le fichier
`/user/<nom>/raw/access.log` préparé au TP2.

Le fichier `compose.yaml` du TP1 a été adapté pour intégrer l'exécution
de YARN dans les conteneurs existants. Il monte désormais les nouveaux
fichiers de configuration et les scripts de lancement nécessaires.

Les fichiers de configuration utilisés sont :
- `config/core-site.xml` : configuration de l'accès à HDFS ;
- `config/hdfs-site.xml` : configuration du système de fichiers HDFS ;
- `config/yarn-site.xml` : configuration de YARN ;
- `config/mapred-site.xml` : configuration de MapReduce pour son exécution
  sur YARN.

Les scripts de lancement ont également été adaptés :
- `scripts/start-namenode.sh` : lancement du NameNode et du ResourceManager
  dans le conteneur `hadoop-namenode` ;
- `scripts/start-datanode.sh` : lancement du DataNode et du NodeManager
  dans chacun des conteneurs `hadoop-datanode1` et `hadoop-datanode2`.

Aucun nouveau conteneur ni fichier `compose.yaml` spécifique au TP3
n'est nécessaire : le cluster existant est simplement étendu pour
permettre l'exécution des applications MapReduce sur YARN.

# Partie A --- Comprendre MapReduce

Au TP2, HDFS permettait de stocker les données. Ici, nous ajoutons le
calcul distribué :

``` text
HDFS → Mapper → Shuffle/Sort → Reducer → HDFS
```

### Exemple

Pour les logs, un Mapper peut produire :

``` text
404    1
404    1
200    1
500    1
```

puis le Shuffle/Sort regroupe :

``` text
200    [1]
404    [1,1]
500    [1]
```

et le Reducer produit :

``` text
200    1
404    2
500    1
```

# Partie B --- Préparer Hadoop Streaming

Entrez dans le NameNode :

``` bash
docker exec -it hadoop-namenode bash
```

Vérifiez Hadoop Streaming :

``` bash
ls /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming*.jar
```

Puis :

``` bash
export STREAMING_JAR=$(ls /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming*.jar | head -n 1)
echo "$STREAMING_JAR"
```

Depuis la racine du dépôt, copiez les scripts :

``` bash
docker cp tp3/scripts/status_mapper.py hadoop-namenode:/tmp/status_mapper.py
docker cp tp3/scripts/status_reducer.py hadoop-namenode:/tmp/status_reducer.py
docker cp tp3/scripts/ip_mapper.py hadoop-namenode:/tmp/ip_mapper.py
```

Puis :

``` bash
docker exec hadoop-namenode chmod +x   /tmp/status_mapper.py   /tmp/status_reducer.py   /tmp/ip_mapper.py
```

# Partie C --- Tester Mapper et Reducer séparément

## C1. Tester le Mapper

``` bash
echo '10.0.0.13 - - [12/Mar/2026:09:03:41 +0000] "GET /products/99 HTTP/1.1" 404 721 "-" "Mozilla/5.0"' | /tmp/status_mapper.py
```

### Question 1

Quelle sortie obtenez-vous ?

### Question 2

Pourquoi le Mapper ne calcule-t-il pas directement le total des `404` ?

## C2. Tester le Reducer

``` bash
printf '404\t1\n404\t1\n404\t1\n' | /tmp/status_reducer.py
```

### Question 3

Quelle sortie obtenez-vous ?

### Question 4

Quelle opération réalise le Reducer ?

# Partie D --- Premier job MapReduce

Vérifiez les données :

``` bash
hdfs dfs -ls /user/<nom>/raw
hdfs dfs -cat /user/<nom>/raw/access.log | head
```

### Question 5

Le job lit-il le fichier depuis le disque local du NameNode ou depuis HDFS ?

### Important

Vérifiez que la sortie n'existe pas :

``` bash
hdfs dfs -test -e /user/<nom>/mr-output/status
echo $?
```

Si nécessaire :

``` bash
hdfs dfs -rm -r /user/<nom>/mr-output/status
```

### Question 6

Pourquoi Hadoop refuse-t-il normalement d'écrire dans un répertoire de sortie déjà existant ?

Lancez le job :

``` bash
hadoop jar "$STREAMING_JAR" \
  -files /tmp/status_mapper.py,/tmp/status_reducer.py \
  -input /user/<nom>/raw/access.log \
  -output /user/<nom>/mr-output/status \
  -mapper "python3 status_mapper.py" \
  -reducer "python3 status_reducer.py"
```

**Observez la sortie** :

``` bash
hdfs dfs -ls /user/<nom>/mr-output/status
hdfs dfs -cat /user/<nom>/mr-output/status/part-*
```

### Question 7

Pourquoi la sortie est-elle un répertoire et non un fichier unique ?

### Question 8

Que représentent les fichiers `part-*` ?

# Partie E --- Comprendre Shuffle et le nombre de Reducers

Pour les statuts :

``` text
200 200 404 500 404 200
```

### Question 9

Complétez le résultat du Mapper. Complétez le regroupement Shuffle/Sort.
Complétez le résultat du Reducer.

Supprimez l'ancienne sortie :

``` bash
hdfs dfs -rm -r /user/<nom>/mr-output/status
```

Relancez avec deux reducers :

``` bash
hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.reduces=2 \
  -files /tmp/status_mapper.py,/tmp/status_reducer.py \
  -input /user/<nom>/raw/access.log \
  -output /user/<nom>/mr-output/status \
  -mapper "python3 status_mapper.py" \
  -reducer "python3 status_reducer.py"
```

Puis :

``` bash
hdfs dfs -ls /user/<nom>/mr-output/status
```

### Question 10

Combien de `part-*` observez-vous ?

# Partie F --- Deuxième traitement : requêtes par IP

Cette fois, nous voulons :

``` text
IP → nombre de requêtes
```

Le Mapper `ip_mapper.py` produit :

``` text
10.0.0.13    1
```

Testez-le :

``` bash
echo '10.0.0.13 - - [12/Mar/2026:09:03:41 +0000] "GET /products/99 HTTP/1.1" 404 721 "-" "Mozilla/5.0"' | /tmp/ip_mapper.py
```

Supprimez une ancienne sortie **si nécessaire** :

``` bash
hdfs dfs -rm -r /user/<nom>/mr-output/by-ip
```

Lancez :

``` bash
hadoop jar "$STREAMING_JAR" \
  -files /tmp/ip_mapper.py,/tmp/status_reducer.py \
  -input /user/hoda/raw/access.log \
  -output /user/hoda/mr-output/by-ip \
  -mapper "python3 ip_mapper.py" \
  -reducer "python3 status_reducer.py"
```

Affichez :

``` bash
hdfs dfs -cat /user/<nom>/mr-output/by-ip/part-*
```

# Partie G --- Compteurs et diagnostic

Après un job, observez les compteurs affichés dans le terminal,
notamment :

-   nombre de lignes d'entrée ;
-   sorties du Mapper ;
-   entrées du Reducer ;
-   sorties du Reducer ;
-   octets lus/écrits.

# Partie H --- Mini-défi : Développer une application Java MapReduce

Dans cette partie, vous allez développer une application MapReduce en
Java en utilisant directement l'API Hadoop. Cette application ne
nécessite pas Hadoop Streaming : elle sera compilée sous forme de
fichier JAR puis exécutée avec `hadoop jar`.

## H1. Besoin fonctionnel

À partir du fichier `/user/<nom>/raw/access.log`, développer une
application qui calcule le nombre de requêtes correspondant aux trois
codes HTTP d'erreur suivants :

-   `403` : Forbidden ;
-   `404` : Not Found ;
-   `500` : Internal Server Error.

Les autres codes HTTP doivent être ignorés.

Le résultat attendu est de la forme :

``` text
403    nombre
404    nombre
500    nombre
```

## H2. Concevoir l'application MapReduce

L'application doit comporter les composants suivants :

1.  **Mapper :** lire chaque ligne du fichier de logs, extraire le code
    HTTP et ne conserver que les codes `403`, `404` et `500`. Pour
    chaque occurrence, produire un couple `(code, 1)`.
2.  **Combiner :** effectuer une première agrégation locale des valeurs
    associées à chaque code HTTP afin de réduire le volume de données
    transférées vers les Reducers.
3.  **Reducer :** calculer le nombre total d'occurrences de chaque code
    HTTP.

Exemple de sortie du Mapper :

``` text
404    1
403    1
404    1
500    1
404    1
```

Exemple de résultat après agrégation :

``` text
403    1
404    3
500    1
```

**Exigences :** 
- Le Combiner doit être explicitement activé dans la configuration du Job Java avec `setCombinerClass()`. 
- Le Job doit être configuré avec **trois Reducers**, soit un nombre égal aux trois codes
d'erreur étudiés, à l'aide de `setNumReduceTasks(3)`. 
- Le nombre de Reducers ne garantit pas, à lui seul, qu'un code HTTP sera affecté à un
Reducer précis. La répartition dépend du Partitioner utilisé. La répartition personnalisée fera l'objet de la question facultative.

## H3. Implémentation et exécution

1.  Créer un projet Java utilisant l'API Hadoop MapReduce.
2.  Implémenter les classes nécessaires à l'exécution du job.
3.  Implémenter la classe principale qui configure et lance le Job.
4.  Configurer explicitement le Combiner avec `setCombinerClass()`.
5.  Configurer le nombre de Reducers à trois avec
    `setNumReduceTasks(3)`.
6.  Compiler l'application et générer un fichier JAR.
7.  Exécuter le Job sur le fichier HDFS :

``` bash
hadoop jar <application.jar> <classe_principale> \
  /user/<nom>/raw/access.log \
  /user/<nom>/mr-output/errors
```

8.  Consulter les résultats :

``` bash
hdfs dfs -ls /user/<nom>/mr-output/errors
hdfs dfs -cat /user/<nom>/mr-output/errors/part-r-*
```

**Attention :** le répertoire de sortie ne doit pas exister avant le lancement du Job. Si nécessaire, supprimez-le avec :

``` bash
hdfs dfs -rm -r /user/<nom>/mr-output/errors
```

### Questions

**Question 11.** Quel est le rôle du Mapper dans cette application ?

**Question 12.** Pourquoi le Combiner permet-il de réduire le volume de données transférées pendant le Shuffle ?

**Question 13.** Pourquoi peut-on utiliser une opération de somme dans le Combiner sans modifier le résultat final ?

**Question 14.** Combien de fichiers `part-r-*` sont normalement générés avec trois Reducers ? Que représente chacun de ces fichiers ? Les trois fichiers contiennent-ils nécessairement chacun un code HTTP différent ?
Justifiez.

------------------------------------------------------------------------

## Partie I --- Question facultative à réaliser à la maison : Personnaliser la répartition avec un Partitioner

On souhaite maintenant étudier une autre règle de répartition des
couples clé/valeur, en utilisant **deux Reducers** :

-   **Reducer 0 :** reçoit les codes HTTP de la famille `4xx` ;
-   **Reducer 1 :** reçoit les codes HTTP de la famille `5xx`.

Pour garantir cette répartition, vous devez développer un Partitioner
personnalisé.

### Travail demandé

1.  Développer une classe Java personnalisée héritant de `Partitioner`.
2.  Implémenter la méthode `getPartition()` pour appliquer la règle de
    répartition demandée.
3.  Configurer le Job pour utiliser cette classe avec
    `setPartitionerClass()`.
4.  Configurer le nombre de Reducers à deux avec `setNumReduceTasks(2)`.
5.  Exécuter l'application et examiner les fichiers `part-r-*`.

### Questions facultatives

**Question 15.** Pourquoi faut-il utiliser un Partitioner personnalisé pour garantir cette répartition ?

**Question 16.** Que pourrait-il se passer si vous conserviez le Partitioner par défaut de Hadoop ?

**Question 17.** Quel est le contenu de chacun des deux fichiers `part-r-*` ? Expliquez pourquoi les codes `4xx` et `5xx` sont séparés.
