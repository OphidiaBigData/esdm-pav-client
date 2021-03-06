import click
import sys
import os
from esdm_pav_client import Workflow

previous_dir = os.path.dirname(os.getcwd())
sys.path.insert(0, os.path.dirname(previous_dir))
sys.path.insert(0, "..")


def verbose_check_display(verbose, text):
    if verbose:
        click.echo(text)


def print_help():
    ctx = click.get_current_context()
    click.echo(ctx.get_help())
    ctx.exit()


@click.command(context_settings=dict(ignore_unknown_options=True,))
@click.option(
    "-v", "--verbose", is_flag=True, help="Will print verbose messages."
)
@click.option(
    "-S", "--server", help="ESDM PAV Runtime address", default="127.0.0.1"
)
@click.option("-P", "--port", help="ESDM PAV Runtime port", default="11732")
@click.option(
    "-m",
    "--monitor",
    is_flag=True,
    help="Display a graph of the experiment workflow execution status",
)
@click.option(
    "-s",
    "--sync_mode",
    is_flag=True,
    help="Change the execution mode to synchronous",
)
@click.option(
    "-c",
    "--cancel",
    help="Cancel the experiment execution. It only works in asynchronous mode",
    type=int,
)
# @click.argument("workflow", type=click.UNPROCESSED)
@click.option(
    "-w",
    "--workflow",
    help="Will run the specified experiment workflow.",
    type=str,
)
@click.argument("workflow_args", nargs=-1, type=click.UNPROCESSED)
def run(
    verbose, server, port, monitor, sync_mode, cancel, workflow, workflow_args
):
    """Command Line Interface to run an ESDM PAV experiment workflow\n
    Example: esdm-pav-client -w experiment.json 1 2"""

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

    if workflow:
        workflow, server, port = modify_args(workflow, server, port)
        args = extract_other_args(workflow_args)
        verbose_check_display(verbose, "Reading the experiment workflow")
        w1 = Workflow.load(workflow)
        if not sync_mode:
            w1.exec_mode = "sync"
            verbose_check_display(
                verbose,
                "Submitting the experiment workflow in synchronous mode",
            )
            w1.submit(server=server, port=port, *args)
            verbose_check_display(
                True,
                "Submitted! Workflow id = {0}".format((str(w1.workflow_id))),
            )
            if monitor:
                w1.monitor()
        else:
            verbose_check_display(
                verbose,
                "Submitting the experiment workflow in asynchronous mode",
            )
            w1.submit(server=server, port=port, *args)
            verbose_check_display(
                True,
                "Submitted! Workflow id = {0}".format((str(w1.workflow_id))),
            )
            w1.monitor(visual_mode=False, frequency=10)

    elif cancel:
        if not sync_mode:
            verbose_check_display(
                verbose,
                "Will cancel the experiment workflow execution: {0}".format(
                    str(cancel)
                ),
            )
            w1 = Workflow(name="sample_workflow")
            w1.workflow_id = cancel
            w1.cancel()
            return
    else:
        print_help()


if __name__ == "__main__":
    run()
