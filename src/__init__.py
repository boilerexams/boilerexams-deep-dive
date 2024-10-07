import inspect
import os

os.environ["BX_SRC_DIR"] = os.path.dirname(
    os.path.abspath(inspect.getsourcefile(lambda: 0))
)


from .database import *  # noqa: F403
