import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECTS_PATH = os.path.join(ROOT_DIR, "projects")


def get_project_path(name: str) -> str:
    return os.path.join(PROJECTS_PATH, name)
