# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys
from datetime import date

project = "Boilerexams Deep Dive"
copyright = f"2024-{date.today().year}, Liam Robinson"
author = "Liam Robinson"
version = "0.0.1"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.viewcode",
    "sphinx-prompt",
    "sphinx_gallery.gen_gallery",
    "sphinx_copybutton",
]
templates_path = ["_templates"]
html_css_files = [
    "css/custom.css",
]
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "pydata_sphinx_theme"
html_static_path = ["_static"]
# html_favicon = "_static/sid_logo_dark.ico"

# html_theme_options = {
#     "logo": {
#         "image_light": "_static/sid_logo_light.png",
#         "image_dark": "_static/sid_logo_dark.png",
#         "text": "MIRAGE",
#     }
# }

html_show_sourcelink = False

# Gallery options
sphinx_gallery_conf = {
    "gallery_dirs": ["gallery"],
    "examples_dirs": ["../../examples"],
    "filename_pattern": "/*.py",
    "download_all_examples": False,
    "image_scrapers": ["matplotlib"],
    "matplotlib_animations": True,
    "thumbnail_size": (333, 250),
    "image_srcset": ["2x"],
}

sys.path.insert(0, os.path.abspath(os.path.join("..", "..")))
