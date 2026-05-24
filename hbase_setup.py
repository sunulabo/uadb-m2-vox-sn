# hbase_setup.py — Tables HBase pour Vox-SN
import happybase
import logging
import subprocess

logger = logging.getLogger('HBaseSetup')

def create_namespace():
    cmd = ['docker', 'exec', 'hbase', 'hbase', 'shell']
    hbase_cmd = "create_namespace 'vox'\nexit\n"
    result = subprocess.run(cmd, input=hbase_cmd, capture_output=True, text=True, timeout=30)
    if 'NamespaceExistException' in result.stdout:
        logger.info("Namespace vox deja existant")
    else:
        logger.info("Namespace vox cree")

def create_vox_tables():
    create_namespace()
    conn = happybase.Connection('localhost', port=9090, timeout=10000)
    conn.open()
    tables = {
        'vox:posts': {
            'meta': {'max_versions': 1},
            'nlp':  {'max_versions': 1},
            'privacy': {'max_versions': 1},
        },
        'vox:alertes': {
            'alerte': {'max_versions': 1, 'time_to_live': 259200},
        },
        'vox:sentiment_agg': {
            'stats': {'max_versions': 48},
        },
    }
    existantes = [t.decode() for t in conn.tables()]
    for nom, fam in tables.items():
        if nom not in existantes:
            conn.create_table(nom, fam)
            logger.info(f'Table {nom} creee')
        else:
            logger.info(f'Table {nom} deja existante')
    conn.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    create_vox_tables()
