🔷 Jour 2 : Idempotence et Run Management
Objectif
Rendre le pipeline re-lançable sans dupliquer les données.

Exercice
Modifier le pipeline pour :

Accepter un run_id en paramètre.

Sauvegarder les résultats dans output/run_id=<date>/result.parquet.

Tester avec le même run_id deux fois (doit écraser les données existantes).

Tester avec des run_id différents (doit créer de nouveaux fichiers).

💡 Apprentissage
Gestion des exécutions multiples.

Isolation des runs.