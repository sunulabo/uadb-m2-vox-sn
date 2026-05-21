# Dossier `models/`

Ce dossier contient les artefacts ML générés par les pipelines d'entraînement :

- `sentiment_lr/` — modèle de classification de sentiment (Spark ML PipelineModel)
- `category_rf/` — modèle de classification de catégorie (Random Forest)
- `confusion_matrix_*.png` — matrices de confusion produites par les notebooks
- `metrics.json` — métriques de la dernière promotion

Les modèles sont également enregistrés dans **MLflow Registry** (`http://localhost:5000`).

## Génération
```bash
# Via Airflow (production)
docker exec -it vox_airflow airflow dags trigger vox_sn_retrain_dag

# Via script (développement)
spark-submit spark/train_classifier.py

# Via notebook
jupyter notebook notebooks/02_modele_ml.ipynb
```

## Promotion
Un modèle n'est promu en `Production` dans MLflow Registry que si **F1 macro ≥ 0.70** sur le test set.
Cette logique est implémentée dans `dags/vox_sn_retrain_dag.py`.
