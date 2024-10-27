Installation
............

After cloning, make a virtual environment with ``python3 -m venv .venv`` and activate it with ``source .venv/bin/activate``.

# Need to add stuff for installing postgres

Install the requirements with ``pip install uv && uv pip install -r requirements.txt``.

Downloading local table dumps
.............................

Get your ``.env.secret`` file set up by Liam. Run ``python pull_tables.py`` to pull a local dump of the database. Without the "Submission" table it takes a few seconds, with it you might be waiting 2-3 minutes.

Building your own scripts
.........................

Start with something like this

.. code-block:: python
    
    import src

    dfs = src.load_tables() # returns as dict[table_name, pl.DataFrame]
    # df = src.load_tables('Submission') return as pl.DataFrame if you only want a particular table, name it

    print(dfs['exam]) # Take a look at the schema and go from there!


Build a local copy of the documentation
.......................................

Run ``make docs``