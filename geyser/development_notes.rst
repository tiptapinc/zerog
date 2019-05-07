************************
Geyser Development Notes
************************

Testing
=======
Tests are written with pytest. In order to use the modules in the main code folder, install geyser locally in edit mode.

In the root directory run:

.. code-block:: shell

    pip install -e .

Imports are done by appending `geyser` to every import.

.. code-block:: python

    import geyser.jobs
