# schema.py — Contrats Pandera pour Vox-SN
import pandera as pa
from pandera.typing import Series
import pandas as pd
import logging
import re

logger = logging.getLogger('VoxSchema')

# Regex de détection des données sensibles (numéros WAVE, OM, téléphone)
PII_PATTERNS = [
    r'\+?221[0-9]{9}',   # Numéro sénégalais
    r'\b7[0-9]{8}\b',    # Mobile local
    r'\b[0-9]{10,16}\b', # Numéro de compte/transaction
]

def contains_pii(text: str) -> bool:
    return any(re.search(p, text) for p in PII_PATTERNS)

class SocialSentimentSchema(pa.SchemaModel):
    post_id: Series[str] = pa.Field(unique=True)
    service_cible: Series[str] = pa.Field(isin=[
        'SENELEC','SEN_EAU','TER','WAVE','ORANGE_MONEY','FREE_MONEY'
    ])
    texte_du_post: Series[str] = pa.Field(str_length={'min_value':5,'max_value':280})
    langue: Series[str] = pa.Field(isin=['FR','WO','EN'])
    timestamp: Series[str] = pa.Field(str_matches=r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$')
    canal: Series[str] = pa.Field(isin=['TWITTER','FACEBOOK','WHATSAPP','RECLAMATION'])

    @pa.check('texte_du_post', name='no_raw_phone_in_text')
    def check_no_pii(cls, series):
        flagged = series.apply(contains_pii)
        if flagged.any():
            logger.warning(f'{flagged.sum()} post(s) contiennent des PII détectés')
        return ~flagged

    class Config:
        strict = True
        coerce = True

def validate_and_filter(df: pd.DataFrame, schema_class) -> pd.DataFrame:
    try:
        return schema_class.validate(df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        err = exc.failure_cases
        logger.warning(f'[{schema_class.__name__}] {len(err)} ligne(s) rejetée(s)')
        valid_idx = df.index.difference(err['index'].dropna().astype(int))
        return df.loc[valid_idx]