# HOW TO RUN

In order to run the tests you need *pytest*

```bash 
pip install -U pytest
```

The you can run:

```python 
python -m pytest
```

# IMPORTANT NOTES

In case of remote execution, setup the values for server/port lines 37-42 in the "test_workflow.py" 

```python 
w1.server = ""
w1.port = ""
w2.server = ""
w2.port = ""
```
