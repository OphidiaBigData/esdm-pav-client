import click
import sys

sys.path.insert(0, "..")
from esdm_pav_client import Workflow


@click.command(context_settings=dict(ignore_unknown_options=True,))
@click.option(
    "-v", "--verbose", is_flag=True, help="Will print verbose messages."
)
@click.option(
    "-S", "--server", help="ESDM PAV Runtime address", default="127.0.0.1"
)
@click.option("-P", "--port", help="ESDM PAV Runtime port", default="11732")
@click.argument("workflow")
@click.argument("workflow_args", nargs=-1, type=click.UNPROCESSED)
def run(verbose, server, port, workflow, workflow_args):
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
    w1.submit(server=server, port=port, *args)


if __name__ == "__main__":
    run()
