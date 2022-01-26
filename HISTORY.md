v1.4.0 - 2021-12-16
-------------------

- This version includes:

  - Usage examples of the module main features
  - Improved README documentation
  - Extended Python client interface and help option
  - Changes in the submit method in Workflow class and to the client for supporting checkpointing feature
  - Changes to the Workflow class constructor to support initialization from running experiment ID and Experiment objects
  - Improvements to monitor method in Workflow class for visualising the more complex constructs (e.g., loops)
  - Improvements to usability by moving the experiment modelling methods to a new Experiment class
  - Changes to support the latest experiment document version


v1.0.0 - 2020-10-01
-------------------

- Initial package release. This version includes:

  - Task and Workflow classes to model a ESDM PAV experiment
  - Workflow class also supports the submission of the PAV experiment on the runtime
  - A python client for the submission of a PAV experiment document in JSON format


