# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
#
# Orquestra Workflow SDK docs aren't published on their own. Rather, they're
# included aspart of multi-repo builds available at docs.orquestra.io. The
# multi-repo build has its own conf.py. However, we're also running isolated
# builds for this repo only to catch errors without having to rebuild the full
# docs suite. We need this file to power the docs build stylecheck.

# -- Project information -----------------------------------------------------

project = "Orquestra Workflow SDK"
copyright = "2023, Zapata Computing, Inc"
author = "your friends at Zapata"

# The full version, including alpha/beta/rc tags
release = "0.0.1"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.inheritance_diagram",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.doctest",
    "sphinx_design",
    "sphinx.ext.graphviz",
    "sphinxcontrib.youtube",
    "sphinxcontrib.autoprogram",
    "sphinx_copybutton",
    "sphinxemoji.sphinxemoji",
    "sphinx_click",
]
source_suffix = {
    ".rst": "restructuredtext",
}

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
    "venv",
    "repos",
    "developer",
    ".venv",
    "internal",
]

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#

html_theme = "furo"

intersphinx_mapping = {"python": ("https://docs.python.org/3", None)}
