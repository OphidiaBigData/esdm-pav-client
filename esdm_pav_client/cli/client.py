import click
import sys
sys.path.insert(0, '..')
from workflow import *

@click.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
))
@click.option('-v', '--verbose', is_flag=True, help="Will print verbose messages.")
@click.option('-w', '--workflow', help="Workflow to run")
@click.option('-u', '--username', help="Username to login to Pyophidia", default='oph-test')
@click.option('-p', '--password', help="Password to login to Pyophidia", default='abcd')
@click.option('-s', '--server', help="Server's address", default='127.0.0.1')
@click.option('-pt', '--port', help="Server's port", default='11732')
@click.pass_context
def run(ctx, verbose, workflow, username, password, server, port):
    def modify_args(workflow, username, password, server, port):
        if workflow.startswith("="):
            workflow = workflow[1:]
        if username.startswith("="):
            username = username[1:]
        if password.startswith("="):
            password = password[1:]
        if server.startswith("="):
            server = server[1:]
        if port.startswith("="):
            port = port[1:]
        return workflow, username, password, server, port

    def extract_other_args(ctx):
        args = []
        for c in ctx.args:
            if c.startswith("="):
                args.append(c[1:])
            elif not c.startswith("-"):
                args.append(c)
        return args

    workflow, username, password, server, port = modify_args(workflow, username, password, server, port)
    args = extract_other_args(ctx)
    if verbose:
        click.echo("Reading the workflow")
    w1 = Workflow.load(workflow)
    if verbose:
        click.echo("Submitting the workflow")
    w1.submit(username=username, password=password, server=server, port=port, *args)

if __name__ == '__main__':
    run()