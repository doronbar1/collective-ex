## How would you improve this code?

This is a real, in-use example from our codebase that regularly sees edits... or at least it was at one point in time. It contains some known flaws and less than optimal patterns. It could really use a good refactoring. How would you improve this code? What would you do differently? How would you test this sample? What else would you need to test?

### Submit a ZIP file containing:

- A source control repo of your preference containing incremental commits. - https://github.com/doronbar1/collective-ex/pull/1
- Starting with the sample code prior to your refactor.
Q - Whatever tests make sense to support the original and your refactor, assuming there is no test coverage currently.
A - I've tried to break most of the logic into smaller functions that it will be relatively easy to test since each function is pretty small. The main function is the engine that pull the tasks that needs to be run, so process name will have his dedicated funtion to execute.

Q - Explanations of why you made the refactors that you did and what you omitted and why.
A - I've tried to remove unnecessary code, as well as improve DB performance while keeping the code readable and understandable. With all of that, I was also trying to break the big function called scheduled_system, into smalled chunks of code which will enable us to test the logic for each type of process name

### Be prepared to present and discuss your decisions!

We'll conduct a mock code review to discuss your proposed refactor. Explain your reasoning, the strategies you employed, and the tradeoffs you considered. There are lots of opportunities for improvement, possibly more than anyone could tackle in the time allotted, and there are pieces of context missing from the example. Such is life.

Rather than grading you on your level of completion, this exercise is designed to:

- Provide a real-world example of what kind of work you can accomplish, not a contrived algorithmic example or toy project from scratch.
- Judge how you deal with ambiguity, missing context, and balancing tradeoffs and your experience with making incremental progress.
- Provide you a glimpse of some of the real work inside the codebase.
- Help us understand the technical value you can bring to the team.

> ### Disclaimer: **None of your code will be used for anything other than this discussion.**
