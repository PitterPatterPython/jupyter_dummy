# jupyter_dummy
A module to help interaction with Jupyter Notebooks and a Fake Dataset called dummy

------
This is a python module that helps to connect Jupyter Notebooks to various datasets. 
It's based on (and requires) https://github.com/JohnOmernik/jupyter_integration_base 



## Initialization 
----

### Example Inits

#### Embedded mode using qgrid
```
from dummy_core.dummy_base import Dummy
dummy_base = Dummy(ipy, debug=False)
ipy.register_magics(dummy_base)
```

