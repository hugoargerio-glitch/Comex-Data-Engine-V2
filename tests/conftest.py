import os
import pytest
from pathlib import Path

# Carrega .env do projeto se existir
env_file = Path(__file__).parent.parent / ".env"
if env_file.exists():
    from dotenv import load_dotenv
    load_dotenv(env_file)
