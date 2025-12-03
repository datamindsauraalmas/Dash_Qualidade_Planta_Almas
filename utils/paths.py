from pathlib import Path

def find_project_root(start: Path | None = None) -> Path:
    """
    Descobre a raiz do projeto subindo pastas até encontrar marcadores.
    Ajuste os marcadores conforme seu projeto.
    """
    start = (start or Path(__file__)).resolve()
    markers = {".env", "requirements.txt", "requirements-lock.txt"}
    for parent in [start, *start.parents]:
        if any((parent / m).exists() for m in markers):
            return parent
    # fallback conservador: 2 níveis acima deste arquivo
    return start.parents[2]

ROOT = find_project_root()