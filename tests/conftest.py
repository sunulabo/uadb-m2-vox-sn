"""
tests/conftest.py — Configuration pytest globale
"""
import sys
import os

# Ajouter les modules du projet au PYTHONPATH
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
for sub in ['spark', 'kafka', 'dashboards', 'scripts']:
    sys.path.insert(0, os.path.join(PROJECT_ROOT, sub))
