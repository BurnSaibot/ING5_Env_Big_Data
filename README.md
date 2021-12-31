# ING5_Env_Big_Data
Le but du projet est de pouvoir lire en temps réel des tweets puis de les envoyer dans kafka afin d'ensuite utiliser spark streaming pour faire de l'analyse de sentiment avant d'envoyer le tweet plus le résultat du traitement dans Hive vers un fichier Parquet stocké sur hdfs

Le projet est réparti en 2 parties :
- un producer kafka, qui va, à partir d'une API Twitter, récupérer des tweets en se basant sur une liste de # donnée en configuration, et les envoyé dans kafka dans un topic (queue) précis
- un consumer kafka, qui était sensé pouvoir récupérer les tweets stocké dans un topic kafka puis les transmettre à un serveur hive après avoir réalisé une analyse de sentiment pour chaque tweet.

Les tweets étant ensuite stocké dans des fichiers Parquet, un format optimisé pour faire du traitement de donné à grande échelle à l'aide d'un cluster Hadoop, en utilisant mapReduce, Spark ou encore TEZ.

# Installation

Pour l'installation, il faut commencer par cloner ce repo git, puis enssuite copier les parties du projet qui vous intéressent sur différents noeuds.
Là un des premiers problèmes risque d'apparaitre : les jar utilisés pour communiquer avec les brokers kafka ainsi que ceux pour réaliser le streaming spark sont fortement liés aux versions de spark et kafka utilisées. 
De plus, n'ayant pas accès au cluster ave clequel j'étais sensé réalisé le projet suite à un problème de DNS, j'ai travaillé sur un cluster monomachine sur ma machine de dev, il est donc possible que ces versions ne soient pas les bonnes pour travailler avec votre cluster.
Il faudra potentiellement donc mettre à jour les deux fichier pom.xml dans les deux projets avant de les compiler en utilisant la commande
'mvn clean compile assembly:single' à la racine des deux projets (consumers et producers)

une fois compilés, pour pouvoir utiliser les différents projets on doit alors mettre le fichier de configuration à jour sur chacun des noeuds.

Le producer est malheureusement difficilement parallélisable, on peut l'utiliser à partir de plusieurs noeuds du cluster avec une configuration différentes pour suivres différents #, mais le parallélisme s'arrête ici. Il est possible de suivre différents # sur un même noeud (expliqué dans les commentaires du fichier de configuration).

Pour le consumer, il est possible de l'installer sur plusieurs noeuds à priori puisque chacun des consumer ira piocher dans le topic, qui agit comme une queue.
Malheureuseent pour le moment, lors des tests réalisés sur mon cluster mono machine, je n'ai pas pu réussir à récupérer le contenu de mon topic, le consumer semble dire que le topic est toujours vide et qu'il n'y a rien à streamer, cela pourrait provenir de mon installation du cluster, ou bien du programme.

# Problèmes rencontrés 
- API twitter en projet open source clairement en cours de développement, j'ai même pu faire quelques remarques sur le projet qui ont été prises en compte.
- Problème avec l'accès au cluster, le dns semble mort et il semblerait qu'un groupe d'élève ait été mis au courant de ce problème avec une solution de contournement, mais que ce groupe ne l'ait pas communiqué aux autres élèves. J'ai donc travaillé à partir de mon propre "cluster" monomachine sur ma machine de dev, et ait finalement passer plus de temps à essayer de tout configurer pour que chaque service puisse fonctionner correctement qu'à avancer dans mon dévoleppement.
- Ma santé.

# Améliorations possibls
- Faire en sorte que le consumer fonctionne normalement, ajouter l'analyse de sentiment
- Améliorer l'installation, j'avais dans l'idée de créer un projet parent en utilisant maven et de créer un rpm permettant d'installer le consumer, le producer ou les deux sur un noeud donné
- Ajouter la sécurité, que vous m'avez demander de laisser tomber après vous être rendu compte que le service kafka était inutilisable sur le cluster (j'avais commencé à travailler là dessus en utilisant JAAS, visible dans une autre branche du projet)
- J'avais dans l'idée de permettre plusieurs type de sortie en plus de Hive (dans des fichiers csv par exemple) 

