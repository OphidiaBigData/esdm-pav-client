from esdm_pav_client import Task, Workflow, Experiment
import pytest

"""An experiment object is being created along with some task objects for the
   testing process"""
e1 = Experiment(
    name="Sample_Workflow",
    author="Author_name",
    abstract="Example workflow for testing",
)
t2 = e1.newTask(
    name="mytask1",
    operator="oph_script",
    arguments={"script": "/gfs-data/home/evachlas/sleep.sh"},
    dependencies={},
)
t3 = e1.newTask(
    name="mytask2",
    operator="oph_script",
    arguments={"script": "/gfs-data/home/evachlas/sleep.sh"},
    dependencies={t2: None},
)

e2 = Experiment(
    name="Sample_Workflow_template",
    author="Author_name",
    abstract="Second example workflow for testing",
)
t2 = e2.newTask(operator="oph_if")

w1 = Workflow(experiment=e1)


w2 = Workflow(experiment=10)
# w1.username = ""
# w1.password = ""
# w1.server = ""
# w1.port = ""
# w2.username = ""
# w2.password = ""
# w2.server = ""
# w2.port = ""

@pytest.mark.parametrize(
    ("server", "port"),
    [
        ("sample_server", "sample_port"),
        (None, None),
        (1, 2),
        ("sample_use", None),
        ([], 0),
    ],
)
def test_submit(server, port):
    w1.submit(server=server, port=port)

# First five should pass
@pytest.mark.parametrize(
    ("frequency", "iterative", "visual_mode"),
    [
        (10, True, False),
        (10, True, True),
        (10, False, True),
        (10, False, False),
        (100, True, False),
        ("10", True, False),
        (10, "True", False),
        (10, True, "False"),
        (None, True, False),
    ],
)
def test_monitor(frequency, iterative, visual_mode):
    w2.monitor(
        frequency=frequency, iterative=iterative, visual_mode=visual_mode
    )

def test_cancel():
    w2.cancel()
