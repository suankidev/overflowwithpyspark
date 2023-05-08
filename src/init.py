import os
import sys

from src.ingest.run_ingest_overflow import get

if os.path.exists('src.zip'):
    sys.path.insert(0, 'src.zip')
else:
    sys.path.insert(0, './src')

print(get())
