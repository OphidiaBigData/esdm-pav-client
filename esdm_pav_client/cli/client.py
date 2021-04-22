import click
import sys
# import os
#
# previous_dir = os.path.dirname(os.getcwd())
# sys.path.insert(0, os.path.dirname(previous_dir))
sys.path.insert(0, "..")
from esdm_pav_client import Workflow


@click.command(context_settings=dict(ignore_unknown_options=True,))
@click.option(
    "-v", "--verbose", is_flag=True, help="Will print verbose messages."
)
@click.option(
    "-S", "--server", help="ESDM PAV Runtime address", default="127.0.0.1"
)
@click.option("-P", "--port", help="ESDM PAV Runtime port", default="11732"
)
@click.option(
    "-m", "--monitor", is_flag=True, help="Will display a graph of the workflow"
)
@click.option(
    "-a", "--async_mode", is_flag=True, help="Will change the exec_mode to async"
)
@click.option(
    "-c", "--cancel", is_flag=True, help="Will cancel the workflow. It only works in the async_mode"
)
@click.argument("workflow")
@click.argument("workflow_args", nargs=-1, type=click.UNPROCESSED)
def run(verbose, server, port, monitor, async_mode, cancel, workflow, workflow_args):
    """Command Line Interface to run an ESDM PAV experiment WORKFLOW\n
       Example: client.py experiment.json 1 2"""

    def modify_args(workflow, server, port):
        if workflow.startswith("="):
            workflow = workflow[1:]
        if server.startswith("="):
            server = server[1:]
        if port.startswith("="):
            port = port[1:]
        return workflow, server, port

    def extract_other_args(wf_args):
        args = []
        for c in wf_args:
            if c.startswith("="):
                args.append(c[1:])
            elif not c.startswith("-"):
                args.append(c)
        return args

    workflow, server, port = modify_args(workflow, server, port)
    args = extract_other_args(workflow_args)

    if verbose:
        click.echo("Reading the workflow")
    w1 = Workflow.load(workflow)

    if verbose:
        click.echo("Submitting the workflow")
    if async_mode:
        w1.exec_mode = "async"
    w1.submit(server=server, port=port, *args)
    if async_mode:
        click.echo("Workflow ID: {0}".format(str(w1.workflow_id)))
        if cancel:
            w1.cancel()
    if monitor:
        w1.monitor()


if __name__ == "__main__":
    run()
