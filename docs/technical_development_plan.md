# Plan de Developpement Technique

## 1. But du plan

Ce document transforme le cadrage produit en plan d'execution concret pour le code existant.

Le principe directeur est :
- garder un coeur de debug V2G generique
- ajouter des modules specialises par type d'essai
- renforcer le rapport et le dashboard sans dupliquer la logique

## 2. Etat actuel du code

Le repo dispose deja d'une bonne base :
- detection de fichiers session : `utils/file_detector.py`
- modeles coeur : `core/models.py`
- ingestion ZIP / dossier : `utils/zip_loader.py`
- parsing Dewesoft CSV et brut : `parsers/dewesoft_csv.py`, `parsers/dewesoft.py`
- reconstruction timeline : `core/session_builder.py`, `timeline/reconstructor.py`
- diagnostic generique : `analyzers/diagnostic_engine.py`, `analyzers/source_comparison.py`
- rapport HTML / PDF : `reports/report_generator.py`, `reports/pdf_report.py`, `reports/html_report.py`
- dashboard Streamlit : `app/main.py`
- graphes : `graphs/plot_builder.py`

Le vrai manque aujourd'hui n'est pas un manque de code, mais un manque de structure metier explicite :
- pas assez de separation entre coeur generique et regles scenario
- verdict encore trop heuristique
- rapport encore pas assez pilote par des blocs de preuve standardises

## 3. Architecture cible

Architecture recommandee a court terme :

1. Couche ingestion
- detecter
- classifier
- decrire ce qui est exploitable ou non

2. Couche normalisation
- convertir chaque source en evenements et mesures canoniques
- enrichir les payloads avec metadata metier standard

3. Couche correlation
- aligner les consignes, les calculs borne, les messages protocole et les mesures
- produire des fenetres d'analyse coherentes

4. Couche diagnostic generique
- localiser le premier point de divergence
- produire verdict principal, confiance, preuves et limites

5. Couche diagnostic specialise
- reactif
- FRT
- limitations / derating / consignes

6. Couche restitution
- dashboard premium
- rapport HTML
- rapport PDF

## 4. Changements de structure recommandes

### 4.1 Dossier `analyzers`

Probleme actuel :
- `diagnostic_engine.py` contient a la fois logique generique, heuristiques, narration et une partie quasi metier

Refactor cible :
- garder `diagnostic_engine.py` comme orchestrateur
- ajouter un sous-ensemble de modules focalises

Fichiers a creer :
- `analyzers/diagnostic_context.py`
- `analyzers/divergence_detector.py`
- `analyzers/verdict_builder.py`
- `analyzers/scenario_detector.py`
- `analyzers/scenarios/reactive.py`
- `analyzers/scenarios/frt.py`
- `analyzers/scenarios/power_limit.py`

Responsabilites :
- `diagnostic_context.py`
  - construire un contexte standard de session
  - exposer sources presentes, couverture temporelle, qualite des donnees
- `divergence_detector.py`
  - trouver le premier ecart significatif
  - rattacher chaque ecart a une preuve
- `verdict_builder.py`
  - assembler verdict, confiance, justification, evidences et limites
- `scenario_detector.py`
  - detecter les scenarios applicables
- `scenarios/*.py`
  - contenir les regles specialisees

### 4.2 Dossier `parsers`

Probleme actuel :
- Dewesoft brut est surtout signale, pas encore gere comme pipeline complet

Evolution cible :
- garder `parsers/dewesoft_csv.py` comme parser principal de mesures
- transformer `parsers/dewesoft.py` en point d'entree de resolution Dewesoft

Ameliorations recommandees :
- extraire la logique de matching CSV dans un helper dedie
- ajouter un statut de conversion plus riche
- remonter un manifeste de conversion exploitable par le dashboard et le rapport

Fichiers a creer :
- `parsers/dewesoft_resolver.py`

Responsabilites :
- `dewesoft_resolver.py`
  - associer brut / CSV / fichiers annexes
  - produire un statut `resolved`, `sidecar_found`, `conversion_required`

### 4.3 Dossier `reports`

Probleme actuel :
- le HTML et le PDF sont riches, mais pas encore bases sur un schema de rapport stable

Evolution cible :
- definir un modele de rapport intermediaire
- utiliser ce modele pour HTML et PDF

Fichiers a creer :
- `reports/report_model.py`
- `reports/report_sections.py`

Responsabilites :
- `report_model.py`
  - structure standard du rapport
  - resume executif, sources, divergence, scenario, preuves, recommandations
- `report_sections.py`
  - helpers de rendu independants du support

### 4.4 Dossier `graphs`

Probleme actuel :
- un seul graphe comparatif global

Evolution cible :
- une bibliotheque de graphes standards par besoin metier

Fichiers a creer :
- `graphs/session_overview.py`
- `graphs/reactive_plots.py`
- `graphs/frt_plots.py`

Responsabilites :
- `session_overview.py`
  - graphe consignes vs mesures
  - marqueurs de divergence
- `reactive_plots.py`
  - graphe Q target vs Q mesuree
- `frt_plots.py`
  - graphe tension / frequence / puissance autour de la perturbation

## 5. Fichiers existants a faire evoluer

### 5.1 `utils/file_detector.py`

Objectifs :
- enrichir le resultat de detection
- ne plus seulement lister les fichiers, mais aussi leur statut metier

Ajouts recommandes :
- distinguer `detected`, `parsable`, `needs_conversion`, `support_only`
- rattacher chaque fichier a une famille fonctionnelle
- preparer les infos utiles au dashboard

### 5.2 `core/models.py`

Objectifs :
- ajouter les modeles canoniques qui manquent pour rendre le pipeline plus propre

Ajouts recommandes :
- `DetectedAssetStatus`
- `DiagnosticContext`
- `ScenarioResult`
- `ReportArtifact`

### 5.3 `parsers/dewesoft.py`

Objectifs :
- passer d'un simple warning brut a une vraie resolution de source Dewesoft

Ajouts recommandes :
- retourner un statut de resolution detaille
- exposer si le CSV vient d'un sidecar, d'une conversion ou s'il manque
- faciliter l'affichage dans le dashboard

### 5.4 `analyzers/diagnostic_engine.py`

Objectifs :
- reduire la complexite et faire de ce fichier un orchestrateur

Decoupage recommande :
- preparation contexte
- analyse generique
- detection scenario
- analyse scenario
- consolidation verdict
- construction sortie rapport/dashboard

### 5.5 `reports/report_generator.py`

Objectifs :
- produire un rapport plus stable, moins dependant d'heuristiques inline

Ajouts recommandes :
- section `Contexte de l'essai`
- section `Couverture des donnees`
- section `Point de divergence`
- section `Analyse scenario`
- section `Conclusion de confiance`

### 5.6 `reports/pdf_report.py`

Objectifs :
- aligner le PDF sur la meme structure que le HTML

Ajouts recommandes :
- meme ordre des sections
- meme vocabulaire
- integration des graphes exportes en image

### 5.7 `graphs/plot_builder.py`

Objectifs :
- faire apparaitre visuellement le point de divergence

Ajouts recommandes :
- marqueur du premier ecart
- affichage plus lisible par familles de signaux
- presets selon scenario

### 5.8 `app/main.py`

Objectifs :
- faire du dashboard une surface de pilotage claire

Ajouts recommandes :
- panneau `Etat de la session`
- panneau `Couverture des donnees`
- panneau `Scenario detecte`
- panneau `Point de divergence`
- panneau `Confiance et limites`
- section `Actions recommandees`

## 6. Roadmap de developpement

## Sprint 1 - Fiabiliser l'entree et la visibilite

But :
- que l'utilisateur sache exactement ce qui a ete detecte et ce qui manque

Travaux :
- enrichir `DetectedFiles`
- enrichir `utils/file_detector.py`
- introduire un statut Dewesoft plus detaille
- afficher la couverture des sources dans `app/main.py`
- afficher ces memes informations dans le rapport

Livrable :
- une session ZIP terrain affiche clairement :
  - ce qui a ete trouve
  - ce qui est exploite
  - ce qui bloque l'analyse

## Sprint 2 - Introduire le point de divergence

But :
- rendre le debug plus specifique sans attendre les modules reactif/FRT

Travaux :
- extraire un `divergence_detector`
- calculer un `first_divergence`
- injecter ce resultat dans :
  - le verdict
  - le rapport
  - le dashboard
  - les graphes

Livrable :
- l'application sait dire quand le probleme commence et sur quelle source il apparait

## Sprint 3 - Stabiliser le verdict premium

But :
- sortir une conclusion plus defendable

Travaux :
- extraire un `verdict_builder`
- normaliser :
  - cause probable
  - meilleure piste
  - confiance
  - preuves
  - donnees manquantes
  - recommandation

Livrable :
- le verdict est plus lisible et plus stable d'une session a l'autre

## Sprint 4 - Introduire les scenarios specialises

But :
- ajouter de la specificite metier sans casser le moteur global

Travaux :
- ajouter `scenario_detector.py`
- ajouter module `reactive.py`
- ajouter module `frt.py`
- ajouter module `power_limit.py`

Livrable :
- si un scenario est reconnu, le moteur generic est complete par une analyse specialisee

## Sprint 5 - Rapport client-ready

But :
- faire monter le rapport au niveau d'un document de debug quasi manuel

Travaux :
- introduire `report_model.py`
- aligner HTML et PDF
- integrer graphes par scenario
- ajouter resume executif plus ferme

Livrable :
- rapport HTML/PDF coherent, premium, repetable

## 7. Premier lot de travail recommande

Le meilleur point de depart code est le suivant :

1. Ajouter un contexte d'analyse standard
- creer `DiagnosticContext`
- centraliser statut des sources et couverture des donnees

2. Extraire le point de divergence
- sortir cette logique de `diagnostic_engine.py`
- la rendre visible dans le dashboard et le rapport

3. Mieux structurer Dewesoft
- distinguer `CSV exploitable`, `brut avec sidecar`, `brut non converti`

Ce lot donne un gain immediat :
- debug plus concret
- UI plus rassurante
- base propre pour reactif et FRT

## 8. Plan de tests

Tests a ajouter :

### Detection / ingestion
- ZIP avec CSV Dewesoft seulement
- ZIP avec brut + CSV sidecar
- ZIP avec brut sans CSV
- ZIP avec noms de dossiers imparfaits

### Divergence
- session avec consigne suivie
- session avec consigne non suivie
- session avec limitation borne
- session avec timeout protocole

### Scenarios
- session reactif
- session FRT
- session limitation puissance

### Restitution
- presence des nouvelles sections dans HTML
- presence des nouvelles sections dans PDF
- presence des statuts dans dashboard

## 9. Definition de succes technique

Le plan est reussi si :
- le moteur generique reste utilisable sur toutes les sessions
- les scenarios specialises s'ajoutent proprement
- le rapport et le dashboard racontent la meme histoire
- le lecteur comprend rapidement :
  - quelles sources ont ete exploitees
  - ou commence le probleme
  - quelle conclusion est defendable
