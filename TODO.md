- Clean up the state.rs code
  - Use borrowed reference instead of own reallocated values if possible
  - each layer of the struct should have its own functions to make their functionality more clear, instead of having almost everything on DB struct level
  - Writing unit tests for the functionalities
  - Add documentation 

- Add README.md
