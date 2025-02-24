from dataclasses import dataclass, field

import importlib


@dataclass
class TaskSpec:
    """Specification of a task."""

    task: str
    parameters: dict = field(default_factory=dict)

    # def __post_init__(self):
    #     self.parameters = dict(self.parameters)

    @classmethod
    def from_dict(cls, data: dict):
        """Create a TaskSpec from a dictionary."""
        return cls(**data)


def load_class(class_name: str):
    """Dynamically load a class from its name."""
    module_name = ".models"
    module = importlib.import_module(module_name, package="orchestra")
    cls = getattr(module, class_name, None)
    if cls:
        return cls
    else:
        print(f"Class {class_name} not found in module {module_name}")
