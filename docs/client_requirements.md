# Exigences Client V2G Debug Tool

## 1. Objectif produit

Le client n'attend pas seulement un parseur multi-sources avec un verdict generique.
La cible est un outil de debug session V2G capable de produire un rapport exploitable par un ingenieur validation, avec une conclusion claire, datee, argumentee et presentable au client final.

Le produit doit permettre de :
- charger une session complete sous forme de ZIP ou dossier local
- parcourir tout le contenu utile de la session, y compris les sous-dossiers heterogenes
- corriger automatiquement les cas de structure reelle terrain: noms de dossiers imparfaits, fautes de frappe, melange de logs, captures, acquisitions et exports
- reconstruire une chronologie fiable de la session
- determiner ou commence l'ecart et qui est probablement responsable
- produire un rapport premium en HTML et PDF avec preuves, tableaux, visuels et conclusion

## 2. Donnees d'entree attendues

L'outil doit traiter une session complete pouvant contenir :
- logs borne: EnergyManager, ChargerApp, meter-dispatcher, logs generiques
- traces reseau: PCAP, netlogger, dossiers `pcap` ou `pcaps`
- mesures Dewesoft exportees en CSV
- acquisitions Dewesoft brutes: `.d7d`, `.dxd`, `.dmd`
- captures d'ecran, images, pieces jointes
- fichiers annexes de contexte lies a la campagne d'essai

Contraintes d'entree :
- l'utilisateur ne doit pas reorganiser son ZIP a la main
- l'outil doit explorer toute l'arborescence
- l'outil doit detecter les variantes de nommage reelles comme `Aquisitions` au lieu de `Acquisitions`
- si des CSV Dewesoft existent deja, ils doivent etre utilises directement
- si seuls des fichiers Dewesoft bruts existent, l'outil doit les signaler explicitement et tenter une conversion assistee ou preparee

## 3. Exigences fonctionnelles prioritaires

### 3.1 Ingestion et detection

Le systeme doit :
- detecter automatiquement toutes les familles de fichiers utiles
- afficher un indicateur clair de progression pendant l'analyse
- montrer a l'utilisateur ce qui a ete detecte, ce qui manque, et ce qui n'est pas encore exploitable
- distinguer clairement les fichiers trouves, exploites, ignores et necessitant conversion

### 3.2 Conversion Dewesoft

Regle cible :
- si un CSV exploitable existe pour une acquisition Dewesoft, l'utiliser sans conversion supplementaire
- sinon, preparer automatiquement la conversion du brut en CSV

Version cible attendue par le client :
- support des acquisitions `.d7d`, `.dxd`, `.dmd`
- association intelligente brut <-> CSV si un export partiel existe deja
- journal de conversion clair dans le dashboard et dans le rapport
- mode assiste pour generer un manifeste des conversions manquantes

Limite actuelle a assumer explicitement :
- le projet sait aujourd'hui detecter les fichiers Dewesoft bruts et exploiter les CSV existants
- la conversion binaire native automatique n'est pas encore garantie de bout en bout

### 3.3 Reconstruction et correlation

Le moteur d'analyse doit :
- aligner les evenements logs, protocoles et mesures sur une timeline commune
- relier consigne demandee, recalcul borne, envoi au vehicule et mesure physique reelle
- montrer le premier point de rupture significatif
- mettre en evidence les contradictions entre sources

### 3.4 Verdict technique

Le verdict ne doit pas se limiter a `borne / vehicule / communication`.

Le client attend au minimum :
- une conclusion claire
- un niveau de confiance
- le premier instant ou le probleme devient visible
- la source qui porte la meilleure preuve
- une explication lisible par un humain non developpeur
- les donnees manquantes qui empechent une conclusion plus forte

Le verdict cible doit pouvoir repondre a des formulations du type :
- la borne a bien recu la consigne mais a applique une limitation interne
- la borne a envoye la bonne consigne mais le vehicule n'a pas suivi
- le probleme apparait apres une rupture protocolaire
- les mesures physiques confirment ou contredisent les logs applicatifs

### 3.5 Rapport premium

Le rapport genere doit etre plus proche d'un rapport d'analyse manuel que d'un simple dump technique.

Le rapport HTML/PDF doit inclure :
- page de garde
- resume executif
- contexte session
- inventaire des sources detectees
- verdict principal
- point d'origine du probleme
- chronologie argumentee
- preuves par source
- tableaux de correlation
- graphiques ou visuels utiles
- captures/images de support si presentes
- limites de l'analyse
- recommandations et prochaines actions

Le style attendu :
- structure nette
- sections stables et repetables
- formulation professionnelle
- lisibilite client
- export partageable sans retraitement manuel

### 3.6 Dashboard

Le dashboard doit etre explicite meme pour un utilisateur non technique.

Il doit afficher :
- etat de la session importee
- progression de l'analyse
- sources detectees par categorie
- alertes sur les donnees manquantes
- verdict final
- meilleure piste a verifier
- point de depart du probleme
- acces direct aux rapports HTML/PDF

Le client attend un rendu premium et rassurant, pas seulement fonctionnel.

## 4. Exigences d'analyse metier a renforcer

Le plus gros ecart actuel n'est pas seulement visuel, il est metier.

Le client veut un debug plus specifique, donc l'outil doit evoluer d'un diagnostic generique vers un raisonnement par scenario d'essai.

Capacites a viser :
- reconnaitre le type de campagne ou de scenario
- isoler la fenetre de temps ou l'essai devient non conforme
- comparer la consigne attendue, la limite calculee et la reponse mesuree
- produire des arguments techniques courts mais fermes
- sortir une conclusion `pass/fail + pourquoi`

Exemples de questions metier auxquelles le produit doit repondre :
- est-ce que la borne a applique la bonne logique de limitation
- est-ce que le vehicule respecte la consigne envoyee
- est-ce que la puissance reactive suit la demande
- a quel moment la session diverge
- est-ce que les mesures externes confirment les traces internes

## 5. Ecarts actuels identifies

Etat actuel du projet :
- bonne base de detection multi-sources
- dashboard deja ameliore
- rapport HTML/PDF deja structure
- parser Primara/Dewesoft CSV deja renforce
- fichiers bruts Dewesoft deja detectes

Ecarts a fermer :
- conversion Dewesoft brute pas encore totalement automatisee
- verdict encore trop generique sur certains cas
- raisonnement pas encore organise par scenario metier
- rapport encore insuffisamment proche d'un rapport d'expertise humain
- manque possible de graphiques et de preuves ciblees selon le type d'anomalie

## 6. Definition de succes

On pourra considerer que le produit repond bien au besoin client quand :
- un utilisateur depose un ZIP brut de campagne sans preparation manuelle
- l'application detecte correctement toutes les sources utiles
- elle signale clairement les trous de donnees et les conversions manquantes
- elle identifie un point de depart du probleme avec un niveau de confiance assume
- elle produit un rapport HTML et PDF directement presentable
- ce rapport est suffisamment clair pour reduire fortement le temps de redaction manuelle

## 7. Plan de travail recommande

Ordre de priorite propose :

1. Cadrer les scenarios client reels a couvrir
- lister 5 a 10 cas d'usage metier prioritaires
- definir pour chacun les entrees minimales, les regles de decision et la forme de conclusion attendue

2. Fiabiliser l'ingestion et Dewesoft
- terminer la strategie CSV existant sinon conversion assistee
- tracer clairement les conversions manquantes dans le dashboard et le rapport

3. Renforcer le moteur de diagnostic
- passer d'un verdict global generique a des regles plus explicites par scenario
- mieux localiser le `premier point de divergence`

4. Monter le niveau du rapport
- ajouter plus de graphiques, visuels et sections standardisees
- produire une synthese executive tres claire en tete de document

5. Stabiliser la boucle utilisateur
- toujours montrer `ce que l'outil est en train de faire`
- toujours expliquer pourquoi une conclusion est forte ou faible

## 8. Decision produit a prendre avec le client

Avant de continuer a empiler des ameliorations, il faut verrouiller 4 points :
- quels scenarios d'essai doivent etre couverts en priorite
- quel niveau d'automatisation Dewesoft est reellement attendu
- a qui s'adresse le rapport final: ingenieur debug, chef de projet, client final
- quel format de conclusion est juge acceptable: orientation probable, verdict ferme, ou pass/fail par scenario

## 9. Ligne directrice

La bonne direction n'est pas seulement `plus de parsing`.
La bonne direction est :

un outil qui transforme un dossier de session brut en conclusion technique defendable, avec preuves, visuels et rapport client-ready.

## 10. Positionnement produit recommande

Le point important a clarifier avec le client est celui-ci :

le produit ne doit pas etre un outil `uniquement reactif` ou `uniquement FRT`.

Ce serait trop limite, et ce ne serait pas logique si la vraie promesse du produit est :
- prendre une session complete
- comprendre les logs, PCAP et mesures
- reconstruire ce qui s'est passe
- sortir la bonne conclusion avec les bonnes preuves

### Recommandation

La meilleure architecture produit est un modele hybride :
- un coeur de debug generique commun a toutes les sessions
- des modules d'analyse specialises selon le type d'essai detecte

Autrement dit :
- le socle doit toujours fonctionner pour toute session V2G
- si la session correspond a un essai reactif, on active des regles reactif
- si la session correspond a un essai FRT, on active des regles FRT
- si la session correspond a un autre cas, on garde le moteur generique et ses preuves

### Pourquoi cette approche est la bonne

Elle permet de garder :
- un seul outil
- une seule experience utilisateur
- une seule logique d'import et de reconstruction
- une seule structure de rapport

Et elle permet d'ajouter :
- des verdicts plus fins pour certains essais
- des graphes et controles adaptes au contexte
- des regles metier specifiques sans casser l'outil general

### Ce qu'il faut eviter

Il faut eviter deux extremes :
- un outil 100% generique qui reste flou et donne des conclusions trop faibles
- un outil 100% specifique reactif/FRT qui ne sert plus des qu'on change de campagne

### Formulation simple a utiliser avec le client

On ne construit pas un outil different pour chaque essai.
On construit un moteur de debug V2G general, capable de reconnaitre le contexte d'essai et d'appliquer des analyses specialisees quand c'est necessaire.

### Decision produit recommandee

Le produit cible devient :
- un `V2G Debug Platform`
- avec un tronc commun multi-sources
- et des packs de diagnostic metier par type d'essai

### Consequence directe sur le backlog

Les prochaines evolutions doivent donc etre pensees en 2 couches :

1. Couche generique
- ingestion complete ZIP
- correlation logs / PCAP / mesures
- point de divergence
- verdict principal
- rapport premium

2. Couche scenario
- regles essai reactif
- regles essai FRT
- regles limitations puissance / consigne
- conclusions pass/fail ou cause racine selon le type d'essai

## 11. Backlog produit recommande

Ce backlog est organise pour construire d'abord un socle solide, puis ajouter les analyses specialisees sans casser l'outil global.

## 11.1 P0 - Fondations indispensables

Ces sujets doivent etre stabilises en premier, car ils conditionnent tout le reste.

### P0.1 Ingestion session complete

Objectif :
- accepter un ZIP ou un dossier brut sans preparation manuelle

Taches :
- parcourir toute l'arborescence de session sans supposer une structure parfaite
- detecter les variantes de nommage reelles
- separer les fichiers par famille: logs, PCAP, CSV, Dewesoft brut, images, annexes
- afficher un inventaire complet dans le dashboard

Definition of done :
- un ZIP terrain complet est detecte sans reorganisation manuelle
- l'utilisateur voit clairement ce qui a ete trouve

### P0.2 Statut d'analyse visible

Objectif :
- rendre l'outil rassurant et explicite pendant l'execution

Taches :
- afficher les etapes `import`, `detection`, `timeline`, `mesures`, `diagnostic`, `rapport`
- afficher les sources traitees et celles non exploitables
- afficher les blocages de conversion ou les donnees manquantes

Definition of done :
- l'utilisateur comprend en direct ce que fait l'outil
- il sait pourquoi une analyse est forte, partielle ou incomplete

### P0.3 Dewesoft exploitable

Objectif :
- ne plus perdre les acquisitions Dewesoft utiles

Taches :
- utiliser automatiquement les CSV s'ils existent
- associer intelligemment un brut Dewesoft a un CSV voisin si un export existe deja
- produire un manifeste des conversions manquantes
- afficher dans le dashboard et dans le rapport ce qui est exploite et ce qui reste brut

Definition of done :
- aucune session avec CSV existants ne passe a cote des mesures
- les bruts non convertis sont identifies clairement

## 11.2 P1 - Coeur du debug generique

Cette couche doit marcher pour toutes les sessions, quel que soit le type d'essai.

### P1.1 Timeline multi-sources robuste

Objectif :
- reconstruire une chronologie fiable a partir des logs, protocoles et mesures

Taches :
- normaliser les timestamps
- aligner les sources internes et externes
- identifier le debut, les transitions importantes et la fin d'essai
- gerer les trous de donnees ou les horloges imparfaites

Definition of done :
- la timeline devient la colonne vertebrale du diagnostic

### P1.2 Point de divergence

Objectif :
- trouver le premier moment ou le comportement devient anormal

Taches :
- comparer consigne demandee, consigne recalculee, consigne envoyee et puissance mesuree
- detecter les premiers ecarts significatifs
- rattacher chaque ecart a la meilleure preuve disponible

Definition of done :
- le rapport sait dire quand et ou le probleme commence

### P1.3 Verdict principal premium

Objectif :
- sortir un resultat plus clair qu'un simple label generique

Taches :
- fournir `cause probable`, `niveau de confiance`, `premiere preuve`, `donnees manquantes`
- expliquer la conclusion en francais metier simple
- distinguer conclusion ferme, probable ou insuffisamment prouvee

Definition of done :
- un lecteur non developpeur comprend la conclusion sans lire tout le detail technique

### P1.4 Rapport premium

Objectif :
- produire un rapport directement partageable

Taches :
- page de garde et resume executif
- contexte session et matrice des sources
- chronologie argumentee
- preuves principales
- tableaux de correlation
- graphiques utiles
- captures/images si presentes
- conclusion et recommandations

Definition of done :
- le rapport HTML/PDF peut etre diffuse sans grosse retouche manuelle

## 11.3 P2 - Modules specialises par type d'essai

Ces modules s'appuient sur le coeur generique. Ils ne remplacent pas le moteur principal.

### P2.1 Module essai reactif

Objectif :
- analyser finement les campagnes liees au reactif

Taches :
- reconnaitre un essai reactif
- suivre la demande Q, la reponse mesuree Q et les limitations eventuelles
- verifier la coherence entre consigne, calcul borne et mesure physique
- sortir une conclusion `pass/fail + pourquoi`

Definition of done :
- sur une session reactif, l'outil produit un verdict plus fin que le moteur generique

### P2.2 Module essai FRT

Objectif :
- analyser les comportements pendant un evenement reseau ou une perturbation

Taches :
- reconnaitre une campagne FRT
- identifier la fenetre de perturbation
- suivre tension, frequence, reponse puissance et comportement session
- conclure sur la tenue ou l'echec du comportement attendu

Definition of done :
- sur une session FRT, le rapport structure l'analyse autour de la perturbation

### P2.3 Module limitations / consignes

Objectif :
- mieux traiter les cas de limitation puissance, derating ou logique interne borne

Taches :
- identifier les recalculs internes
- expliquer les plafonnements
- relier la limitation au contexte source

Definition of done :
- on peut distinguer une limitation borne d'un non-suivi vehicule avec plus de certitude

## 11.4 P3 - Experience premium

Cette couche renforce la valeur percue du produit.

### P3.1 Dashboard premium

Taches :
- rendre la synthese plus visuelle
- ajouter cartes de statut, alertes fortes, progression, resume executif
- rendre les preuves principales visibles sans devoir ouvrir le rapport

### P3.2 Bibliotheque de graphiques

Taches :
- ajouter graphes standardises par type d'essai
- afficher sur une meme vue consignes, mesures internes, mesures Dewesoft et evenements
- integrer ces graphes dans HTML et PDF

### P3.3 Traçabilite analyse

Taches :
- journaliser les decisions de diagnostic
- montrer quelles regles ont ete activees
- montrer pourquoi une conclusion est faible ou forte

## 11.5 Priorisation pratique

Ordre de realisation recommande :

1. P0.1 a P0.3
- fiabiliser l'entree, la visibilite d'analyse et Dewesoft

2. P1.1 a P1.4
- rendre le moteur generique vraiment defendable

3. P2.1 puis P2.2
- ajouter reactif puis FRT comme modules specialises

4. P2.3 puis P3
- enrichir les cas de limitation et pousser la presentation premium

## 11.6 Message produit a retenir

Le produit n'est pas :
- un parseur brut
- un outil reserve a un seul type d'essai

Le produit est :
- une plateforme de debug V2G generaliste
- renforcee par des modules d'analyse specialises
- capable de sortir une conclusion premium, argumentee et presentable
