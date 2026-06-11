# V2G Debug Platform

## Specification Client

### 1. Vision produit

Le produit vise a automatiser l'analyse de sessions V2G multi-sources afin de reduire fortement le temps de debug, d'ameliorer la qualite des conclusions techniques et de generer un rapport directement presentable au client final.

L'objectif n'est pas de construire un outil reserve a un seul type d'essai.
L'objectif est de construire une plateforme de debug V2G generique, capable :
- d'ingest un dossier de session brut
- de reconstruire la chronologie technique
- de recouper plusieurs familles de preuves
- de produire un verdict argumente
- d'activer, lorsque c'est pertinent, des analyses specialisees selon le contexte d'essai

### 2. Probleme adresse

Aujourd'hui, une investigation V2G demande souvent :
- d'ouvrir manuellement plusieurs logs
- de recouper des traces PCAP
- d'interpreter des mesures Dewesoft ou Primara
- d'identifier a la main le point de rupture
- de rediger ensuite un rapport de conclusion

Cette approche est lente, fragile et peu reproductible.

La plateforme doit transformer ce travail artisanal en un processus outille, trace et presentable.

### 3. Proposition de valeur

La plateforme apporte quatre benefices principaux :

#### 3.1 Centralisation

Une seule interface pour traiter :
- logs applicatifs borne
- traces protocole
- mesures internes
- mesures externes Dewesoft / Primara
- captures et annexes visuelles

#### 3.2 Acceleration du debug

Le systeme automatise :
- la detection des fichiers utiles
- la reconstruction de la timeline
- la correlation des sources
- la localisation du premier point de divergence

#### 3.3 Qualite des conclusions

Le moteur ne doit pas seulement produire un `avis probable`.
Il doit fournir :
- une conclusion claire
- un niveau de confiance
- les preuves principales
- les limites de l'analyse
- les actions recommandees

#### 3.4 Restitution client-ready

La sortie doit etre exploitable par :
- un ingenieur debug
- un ingenieur validation
- un chef de projet
- un client final non developpeur

### 4. Perimetre fonctionnel cible

#### 4.1 Entrees supportees

La plateforme doit accepter :
- un fichier ZIP de session
- un dossier local de session

Le package peut contenir :
- logs EnergyManager
- logs ChargerApp
- logs meter-dispatcher
- logs generiques
- PCAP et netlogger
- exports Dewesoft / Primara en CSV
- acquisitions Dewesoft brutes `.d7d`, `.dxd`, `.dmd`
- images, captures et annexes

#### 4.2 Capacites coeur

Le coeur de la plateforme doit :
- parcourir automatiquement toute l'arborescence
- detecter les variantes reelles de structure et de nommage
- classifier les sources trouvees
- distinguer les donnees exploitables des donnees incompletes ou non converties
- reconstruire une timeline multi-sources
- correler consignes, mesures et indices protocole
- identifier le premier point de divergence
- produire un verdict principal

#### 4.3 Capacites de restitution

La plateforme doit fournir :
- un dashboard interactif
- un rapport HTML
- un rapport PDF
- une timeline exportable

### 5. Principe d'analyse recommande

Le produit ne doit pas etre un outil ferme sur un seul scenario.

Le principe recommande est :
- un moteur generique commun a toutes les sessions
- des couches d'analyse specialisees activees selon le contexte d'essai detecte

Cela permet :
- de garder un outil unique
- de couvrir plusieurs campagnes d'essai
- d'augmenter la precision sans perdre la genericite

### 6. Sorties attendues

#### 6.1 Dashboard

Le dashboard doit afficher de facon lisible :
- l'etat de la session importee
- les sources detectees
- la couverture des donnees
- le statut Dewesoft
- le point de divergence
- le verdict principal
- les regles d'analyse generiques
- les preuves et limites

#### 6.2 Rapport

Le rapport doit contenir au minimum :
- page de garde
- resume executif
- inventaire des sources
- couverture des donnees
- point de divergence
- chronologie argumentee
- conclusion technique
- regles d'analyse appliquees
- recommandations

### 7. Niveaux de conclusion

Le systeme doit pouvoir produire plusieurs niveaux de sortie selon la qualite des donnees :

#### Niveau 1 - Conclusion forte

Conditions :
- sources coherentes
- mesures disponibles
- ecart bien localise

Sortie attendue :
- conclusion defendable
- confiance moyenne ou elevee
- preuves principales clairement citees

#### Niveau 2 - Conclusion orientee

Conditions :
- plusieurs indices convergents
- mais pas encore toutes les preuves ideales

Sortie attendue :
- piste principale
- limites explicites
- recommandations pour confirmer

#### Niveau 3 - Conclusion insuffisante

Conditions :
- donnees manquantes
- contradiction forte entre sources
- mesures physiques absentes

Sortie attendue :
- impossibilite de trancher fermement
- causes de faiblesse du diagnostic
- actions necessaires pour renforcer l'analyse

### 8. Positionnement vis-a-vis des scenarios d'essai

Le produit n'est pas :
- un outil reserve au reactif
- un outil reserve au FRT
- un outil reserve a un seul ticket projet

Le produit est :
- une plateforme de debug V2G generique
- capable de reconnaitre des contextes d'essai
- et de charger des analyses specialisees lorsque cela apporte un gain reel

Exemples de familles d'analyse specialisees a moyen terme :
- limitation active / consigne non suivie
- reactif
- FRT
- incoherence protocole / physique
- contradiction borne / vehicule

### 9. Etat actuel et ecarts a fermer

#### 9.1 Base deja disponible

Le projet dispose deja :
- d'une base de detection multi-sources
- d'un dashboard ameliore
- d'une generation HTML / PDF
- d'une meilleure prise en charge Dewesoft / Primara CSV
- d'un premier point de divergence
- d'un debut de regles d'analyse generiques

#### 9.2 Ecarts encore visibles

Pour atteindre un niveau client premium, il reste a renforcer :
- la profondeur du moteur de debug
- la segmentation intelligente de session
- la hierarchie des preuves par source
- la stabilite du verdict
- la clarte executive du rapport

### 10. Feuille de route recommandee

#### Phase 1 - Robustesse des donnees

Objectif :
- fiabiliser les entrees et la couverture de session

Travaux :
- detection complete des sources
- statut clair des fichiers exploitables
- meilleure gestion Dewesoft brut / CSV

#### Phase 2 - Moteur generique defendable

Objectif :
- produire une analyse generique utile et credibile

Travaux :
- point de divergence
- regles generiques
- confiance
- limites
- recommandations

#### Phase 3 - Rapport premium

Objectif :
- fournir une restitution directement presentable

Travaux :
- enrichissement du dashboard
- rapport HTML / PDF plus executif
- meilleure mise en avant des preuves

#### Phase 4 - Modules specialises

Objectif :
- augmenter la precision sur certains contextes d'essai

Travaux :
- modules reactif
- modules FRT
- modules limitation / consigne

### 11. Definition de succes

Le produit sera considere comme reussi lorsque :
- un utilisateur pourra deposer un package brut sans preparation manuelle
- la plateforme identifiera correctement les sources et leurs limites
- elle reconstruira une timeline exploitable
- elle localisera le point de divergence
- elle produira une conclusion defendable
- elle generera un rapport client-ready avec un niveau de lisibilite professionnel

### 12. Message simple a porter au client

La plateforme proposee est un moteur de debug V2G generaliste, renforce par des analyses specialisees selon le contexte detecte, afin de transformer un dossier de session brut en conclusion technique argumentee et presentable.
