# train_classifier.py — Classification multi-classe Spark MLlib
# Sentiment (Positif/Négatif) + Catégorie (Tarif/Réseau/ServiceClient/Fraude)
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--output-path', default='models/classifier_latest')
parser.add_argument('--window-days', type=int, default=30)
args = parser.parse_args()

spark = SparkSession.builder \
    .appName('VoxSN_TrainClassifier') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

def load_training_data():
    data = [
        # NEGATIF_FORT - TECHNIQUE
        ('transfert bloque impossible joindre support', 'NEGATIF_FORT', 'TECHNIQUE'),
        ('dafa teye duma genn xaalis bi', 'NEGATIF_FORT', 'TECHNIQUE'),
        ('panne indisponible matin inacceptable reseau', 'NEGATIF_FORT', 'TECHNIQUE'),
        ('coupure depuis heures quand revient electricite', 'NEGATIF_FORT', 'TECHNIQUE'),
        ('bloque sans raison application systeme erreur', 'NEGATIF_FORT', 'TECHNIQUE'),
        ('reseau coupe depuis matin aucune info', 'NEGATIF_FORT', 'TECHNIQUE'),
        ('application plante erreur systeme bloque', 'NEGATIF_FORT', 'TECHNIQUE'),
        ('service indisponible bug erreur connexion', 'NEGATIF_FORT', 'TECHNIQUE'),
        # NEGATIF_FORT - FRAUDE
        ('arnaque escroquerie argent perdu disparu', 'NEGATIF_FORT', 'FRAUDE'),
        ('vol transaction echouee argent debit compte', 'NEGATIF_FORT', 'FRAUDE'),
        ('frais caches arnaque commission abusive', 'NEGATIF_FORT', 'FRAUDE'),
        ('argent perdu transaction jamais recue arnaque', 'NEGATIF_FORT', 'FRAUDE'),
        ('scam escroquerie montant preleve sans autorisation', 'NEGATIF_FORT', 'FRAUDE'),
        ('debit non autorise compte vide arnaque', 'NEGATIF_FORT', 'FRAUDE'),
        # NEGATIF - TARIF
        ('frais trop cher commission orange money', 'NEGATIF', 'TARIF'),
        ('prix commission trop eleve tarif abusif', 'NEGATIF', 'TARIF'),
        ('facture incomprehensible frais anormaux mois', 'NEGATIF', 'TARIF'),
        ('cout transfert excessif cher concurrence', 'NEGATIF', 'TARIF'),
        ('frais mensuels augmentes sans information', 'NEGATIF', 'TARIF'),
        ('tarif cher na trop commission elevee', 'NEGATIF', 'TARIF'),
        # NEGATIF - SERVICE_CLIENT
        ('remboursement jamais recu apres semaines', 'NEGATIF', 'SERVICE_CLIENT'),
        ('support client repond jamais aide', 'NEGATIF', 'SERVICE_CLIENT'),
        ('compte bloque support injoignable', 'NEGATIF', 'SERVICE_CLIENT'),
        ('pas eau depuis jours repond pas', 'NEGATIF', 'SERVICE_CLIENT'),
        ('attente longue aucune reponse reclamation', 'NEGATIF', 'SERVICE_CLIENT'),
        ('reclamation ignoree depuis semaines', 'NEGATIF', 'SERVICE_CLIENT'),
        ('impossible joindre hotline occupee', 'NEGATIF', 'SERVICE_CLIENT'),
        # POSITIF - SERVICE_CLIENT (bon service)
        ('rapide pratique meilleur service money', 'POSITIF', 'SERVICE_CLIENT'),
        ('application amelioree paiement facile', 'POSITIF', 'SERVICE_CLIENT'),
        ('excellent service satisfaction client', 'POSITIF', 'SERVICE_CLIENT'),
        ('support reactif reponse rapide aide', 'POSITIF', 'SERVICE_CLIENT'),
        # POSITIF - TARIF (bon prix)
        ('gratuit transferts abonnes simple facile', 'POSITIF', 'TARIF'),
        ('sans frais transfert instantane', 'POSITIF', 'TARIF'),
        ('tarif reduit promotion avantage client', 'POSITIF', 'TARIF'),
        # POSITIF - TECHNIQUE (bon fonctionnement)
        ('dafa baax dafa yomb fonctionne bien', 'POSITIF', 'TECHNIQUE'),
        ('fiable transferts internationaux rapide', 'POSITIF', 'TECHNIQUE'),
        ('pratique rapide dakar diamniadio top', 'POSITIF', 'TECHNIQUE'),
        ('simple utiliser interface claire rapide', 'POSITIF', 'TECHNIQUE'),
        ('rafet dafa baax service qualite', 'POSITIF', 'TECHNIQUE'),
        ('meilleur application fintech senegal fiable', 'POSITIF', 'TECHNIQUE'),
        ('transfert instantane fonctionne parfaitement', 'POSITIF', 'TECHNIQUE'),
    ]
    return spark.createDataFrame(data, ['texte_clean', 'sentiment_label', 'categorie'])

df = load_training_data()
df.show(5)
print(f'Total données : {df.count()}')

train, test = df.randomSplit([0.8, 0.2], seed=42)
print(f'Train={train.count()} | Test={test.count()}')

label_idx_sent = StringIndexer(inputCol='sentiment_label', outputCol='label_sent')
tokenizer      = Tokenizer(inputCol='texte_clean', outputCol='tokens')
remover        = StopWordsRemover(inputCol='tokens', outputCol='tokens_clean')
hashing_tf     = HashingTF(inputCol='tokens_clean', outputCol='tf', numFeatures=5000)
idf            = IDF(inputCol='tf', outputCol='features')
lr_sent        = LogisticRegression(featuresCol='features', labelCol='label_sent',
                                    maxIter=20, regParam=0.01)

pipeline_sent  = Pipeline(stages=[label_idx_sent, tokenizer, remover,
                                   hashing_tf, idf, lr_sent])
model_sent     = pipeline_sent.fit(train)

label_idx_cat = StringIndexer(inputCol='categorie', outputCol='label_cat')
rf_cat        = RandomForestClassifier(featuresCol='features', labelCol='label_cat',
                                       numTrees=100, maxDepth=6, seed=42)

pipeline_cat  = Pipeline(stages=[label_idx_cat, tokenizer, remover,
                                  hashing_tf, idf, rf_cat])
model_cat     = pipeline_cat.fit(train)

ev = MulticlassClassificationEvaluator(metricName='f1')

preds_sent = model_sent.transform(test)
ev.setLabelCol('label_sent').setPredictionCol('prediction')
print(f'[Sentiment] F1 = {ev.evaluate(preds_sent):.4f}')

preds_cat = model_cat.transform(test)
ev.setLabelCol('label_cat').setPredictionCol('prediction')
print(f'[Catégorie] F1 = {ev.evaluate(preds_cat):.4f}')

model_sent.write().overwrite().save(f'{args.output_path}/sentiment')
model_cat.write().overwrite().save(f'{args.output_path}/categorie')
print(f'Modèles sauvegardés dans {args.output_path}')

spark.stop()