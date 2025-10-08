# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'dagster-slurm'
copyright = '2025, Georg Heiler, Hernan Picatto'
author = 'Georg Heiler, Hernan Picatto'
release = '0.0.1'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',      # Core library to pull documentation from docstrings
    'sphinx.ext.autosummary',  # Create neat summary tables
    'sphinx.ext.napoleon',     # Support for Google and NumPy style docstrings
    'myst_parser',             # To parse .md files
    'sphinx_markdown_builder', # The crucial extension to build markdown files
    'sphinx_autodoc_typehints',
]

autosummary_generate = True
autodoc_typehints = 'description'
autodoc_typehints_format = 'short'

# Configure the extension
typehints_fully_qualified = False
always_document_param_types = True
typehints_document_rtype = True
typehints_use_rtype = True

# Put type hints in signature but format them safely
autodoc_typehints = 'signature'
autodoc_typehints_format = 'short'

templates_path = ['_templates']
exclude_patterns = []



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']


# Custom processing to escape problematic characters
def process_signature(app, what, name, obj, options, signature, return_annotation):
    if signature:
        # Replace <factory> with a safe representation
        signature = signature.replace('<factory>', '\\<factory\\>')
    return signature, return_annotation

def setup(app):
    app.connect('autodoc-process-signature', process_signature)