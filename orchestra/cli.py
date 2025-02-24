import click
import yaml
import importlib


from .meta import TaskSpec, load_class


@click.group()
@click.option(
    "--yaml-file",
    type=click.Path(exists=True),
    default="tasks.yaml",
    help="YAML file containing tasks.",
)
@click.pass_context
def cli(ctx, yaml_file):
    """A CLI tool to manage and run tasks defined in a YAML file."""
    print(f"reading tasks from {yaml_file}...")
    with open(yaml_file, "r") as file:
        ctx.obj = yaml.safe_load(file)  # Load tasks into context object


@cli.command()
@click.pass_context
def list(ctx):
    """List available tasks from the YAML file."""
    task_list = ctx.obj
    for task_spec in task_list:
        d = TaskSpec.from_dict(task_spec)
        print("d=", d)
        c = load_class(d.task)
        if c is None:
            print(f"Task named {d.task} not found.")
            continue
        print(c)
        ii = c(**d.parameters)
        print("instance", ii)
        print(ii.get_hash())
        print("---")


@cli.command()
@click.argument("task_name")
@click.pass_context
def run(ctx, task_name):
    """Run a specific task by name."""
    tasks = ctx.obj

    # Find the matching task
    task_data = next((t for t in tasks if t["task"] == task_name), None)
    if not task_data:
        click.echo(f"Task '{task_name}' not found in YAML.")
        return

    # Load task class dynamically
    class_name = task_data["task"]
    parameters = {
        param_name: param_value
        for param in task_data.get("parameters", [])
        for param_name, param_value in param.items()
    }

    try:
        task_class = globals().get(class_name)  # Assuming classes are globally defined
        if not task_class:
            click.echo(f"Task class '{class_name}' not found.")
            return

        task_instance = task_class(**parameters)
        task_instance.run()
    except TypeError as e:
        click.echo(f"Error initializing task '{class_name}': {e}")


if __name__ == "__main__":
    cli()
