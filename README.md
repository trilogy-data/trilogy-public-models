# trilogy-public-models

## Model setup

All models should be in a double nested directory; first the platform and then the semantic label of the model

Models should have the following

- entrypoint.preql
- README.md


## Model Tests

All models will be imported and verified. Validation methods will depend on the defined backend. 

All models require that the datasets being shared with the preql validation account. 

Current verifications:

 - model imports successfully
 - datasource bindings exist
 - datasource to concept mappings are appropriately typed
 - concept relations are consistently typed
