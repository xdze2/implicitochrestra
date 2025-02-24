# implicitochrestra


### Spec
- Cache computation for rapid iteration
- Configurable from file (no input from command line), for reproducibility 
- Implicit dependencies (dependencies are known at run time)

### Lim
- model implementation need the task framework... -> query interface

### Concepts
- Task:
 * class: its a machine to do something
 * instance: include parameters and configuration
 * run: results of computation of an instance 


## CLI


orch run graph132
orch run -i task.yaml graph132
orch run -i task.yaml --all

orch list # list Tasks
orch show MakeGraph 


orch list-runs 






