import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

# constants.py reads os.environ['USER'] at import time; provide a fallback on
# Windows (where the variable is USERNAME) so autodoc can import the package.
os.environ.setdefault("USER", os.environ.get("USERNAME", "sphinx"))

project = "automagician"
copyright = "2024, Ryan Kuang"
author = "Ryan Kuang"
release = "0.1.0"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "furo"
html_static_path = []

# autodoc
autodoc_member_order = "bysource"
autodoc_typehints = "description"
autodoc_typehints_description_target = "documented"

# napoleon (Google-style docstrings)
napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_use_param = True
napoleon_use_returns = True
napoleon_use_rtype = False
