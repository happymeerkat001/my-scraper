Claude Code tip and tricks

# ⭐ Claude Code — Tips & Tricks (Non-Redundant, Easy to Understand)



🔷 1. Planning & Workflow

These tips prevent chaos and keep Claude predictable.

1.1 Use Plan Mode First
	•	Press Shift+Tab or /plan
	•	Claude generates a plan without modifying code
	•	Review → edit → approve → THEN execute

1.2 Use a planning .md file (recommended)

You can:
	•	Tell Claude:
“Write your plan to plan.md”
	•	Edit that plan yourself
	•	Add stop-points
	•	Tell Claude:
“Read plan.md and execute the plan.”

This gives maximum control.

1.3 Use Git as a checkpoint system

Claude has no “undo” like Cursor.
So:
	•	Commit before big tasks
	•	Commit after Claude finishes a batch
	•	If necessary:
git restore or git revert

⸻

🔷 2. Project Configuration & Initialization

These tips help Claude understand your codebase.

2.1 Create Claude.md using /init

This file gives Claude:
	•	project rules
	•	coding preferences
	•	workflows
	•	guardrails

It’s your repo’s memory.

2.2 Keep Claude.md short (only behavior + rules)

Claude loads it every time — shorter = smarter.

⸻

🔷 3. Context Feeding

These tips help Claude understand your environment.

3.1 Use Screenshots

Drag/drop images:
	•	error messages
	•	logs
	•	UI states
	•	DOM screenshots

Claude explains visual bugs extremely well.

3.2 Drag entire folders in

Useful for:
	•	comparing projects
	•	sharing shared utilities
	•	reviewing sibling repos

Claude can modify external folders only if you allow it.

3.3 Paste URLs

Claude can:
	•	read documentation pages
	•	Google search
	•	follow links
	•	cite API behavior

This helps with unfamiliar libraries.

⸻

🔷 4. Execution Features

These make workflows faster inside Claude Code.

4.1 Bash Mode

Run shell commands directly:
	•	read/write files
	•	grep
	•	git add/commit
	•	run Node scripts
	•	install packages

No need to leave Claude.

4.2 Auto-Accept Mode

Claude proceeds without asking confirmation every time.
Use only when you’re confident.

4.3 Message Queue

Queue tasks while Claude is still running:
	•	“When done, refactor utils.js”
	•	“Then run tests”
	•	“Then update README”

⸻

🔷 5. Model Selection

Choose the right Anthropic model for the job.

5.1 /model opus

Use for:
	•	debugging
	•	architecture
	•	planning
	•	complicated reasoning

5.2 /model sonnet

Use for:
	•	routine edits
	•	simple coding
	•	file generation
	•	cleaning up code

5.3 Use Opus Plan Mode

Best speed/performance balance:
	•	Opus writes the plan
	•	Sonnet implements it

⸻

🔷 6. Debugging & Testing

These tips improve reliability.

6.1 Ask Claude to generate tests first (TDD)

Useful for:
	•	new utilities
	•	scrapers
	•	pure logic
	•	input/output functions

6.2 Ask Claude to double-check its own work

Examples:
	•	“Review your work for edge cases.”
	•	“Check for regression risks.”
	•	“Verify no unused code.”

6.3 Use escape key to interrupt Claude

When you need to:
	•	stop execution
	•	redirect
	•	change tasks

⸻

🔷 7. Advanced Power Tools

7.1 Use Subagents for large work

Tell Claude:

“Spin up subagents to evaluate multiple solutions.”

Useful for:
	•	comparing architectures
	•	exploring algorithms
	•	resolving complex bugs
	•	trying 3–4 approaches in parallel

7.2 Use long markdown files with @ references

Example:
	•	Write long requirements into requirements.md
	•	Then say:
“@requirements.md — implement section 3 only.”

This keeps prompts clean and Claude focused.

⸻

🔷 8. Code Review Practices

8.1 Never blindly accept Claude’s code

Treat it like:
	•	A PR from a junior dev
	•	Always review
	•	Always understand
	•	Always run tests

8.2 Ask Claude for a PR-style explanation

Example:
	•	“Explain what changed and why.”
	•	“Show diff summary.”

⸻

🟦 Final Clean Combined List (For Printing)

Claude Code Tips & Tricks
	1.	Use plan mode first
	2.	Edit plans in MD files
	3.	Use Git as checkpoints
	4.	Use screenshots
	5.	Drag folders for context
	6.	Paste URLs to docs
	7.	Use subagents
	8.	Let Claude double-check itself
	9.	Always review code manually
	10.	Create Claude.md with /init
	11.	Keep Claude.md short (behavior only)
	12.	Use to-do lists for complex tasks
	13.	Use Bash Mode
	14.	Use message queue
	15.	Switch models with /model
	16.	Use Opus for planning, Sonnet for implementation
	17.	Use TDD (ask for tests first)
	18.	Use Esc to interrupt
	19.	Use @markdown files for long instructions





# Details 
1. Use Plan Mode First (1:51): Always start by using plan mode (Shift+Tab) to let Claude Code think through the problem and generate a game plan without modifying any code. Review this plan thoroughly and approve or revise it before execution.
2. Generate a Claude MD file (2:36): Create a claude.md file using /init in your codebase. This file acts as Claude's brain and memory, similar to Cursor rules, and it's crucial for providing context and specific instructions to the AI.
3. Commit Frequently and Use Git as a Checkpoint System (3:08): Since Claude Code lacks a built-in restore feature like Cursor, use Git to commit changes frequently. This allows you to revert or discard commits if Claude produces undesirable results.
4. Use Screenshots (3:54): Drag and drop screenshots into Claude Code to provide visual context, such as error messages or design mockups.
5. Drag in Entire Folders (from other codebases) (4:06): Provide additional context by dragging in folders from other related codebases. This can help Claude understand how different parts of a system interact and even make changes to those external folders if given permission.
6. Give it URLs (4:41): Claude Code has web browsing capabilities. You can paste documentation links, and it will read and use that information. It can also perform Google searches to find documentation on its own.
7. Use Sub-agents (5:13): For massive tasks, prompt Claude Code to spin up sub-agents. These instances run in parallel with their own context, significantly speeding up complex operations by breaking them down into smaller, simultaneous tasks.
8. Ask it to Double-Check its Work (5:47): After Claude Code completes a task, ask it to review its work for potential issues, edge cases, or unintended side effects. This can help identify problems you might have missed.
9. Always Review the Code it Generates (6:05): Despite Claude Code's high quality output, always review the generated code as if it were a pull request from another developer. Do not blindly accept its solutions.

-I tell Claude Code to
write its plan to an MD file. I can
then make changes myself or tweak
the plan. Or sometimes I will start by
writing the MD file myself and put in
what I know or what I want to see
done (like specific file names or
function names if I'm being
picky,...), then I use Claude Code to
refine the plan in the MD file or
research specific topics or best
practices in it. When I am ready I
can tell to read the MD and execute
the plan. I can even put in stop
points where I want it to stop and


1. **Initialization and Configuration**
   - To start using Claude, navigate to your directory and type `Claude` in your terminal. Use `--resume` to continue previous sessions.
   - Use the `Claude.md` file as your project’s memory. It stores all workflow rules and preferences, ensuring Claude follows them automatically.

2. **Efficiency Features**
   - Leverage to-do lists to manage complex tasks. Claude automatically checks off completed items, preventing it from getting stuck in loops.
   - Use Bash Mode to run commands without leaving Claude, including reading and writing files or handling Git commands.

3. **Model Utilization**
   - Switch models with `/model` command. Use Opus for complex tasks and Sonnet for routine ones. Consider “Opus Plan” mode for powerful analysis and efficient execution.

4. **Debugging and Testing**
   - Use screenshots to provide context for debugging. Claude can analyze UI issues from provided images.
   - Request Claude to generate tests before implementing features as part of test-driven development (TDD).

5. **Project Management**
   - Use auto-accept mode for seamless changes without repeated confirmations.
   - Implement a message queue to input requests while Claude completes ongoing tasks.

6. **Advanced Utilization**
   - Plan complex tasks using "planning mode" before execution. Claude provides a reviewable plan prior to coding.
   - Use parallel subagents to explore multiple solutions simultaneously, enhancing problem-solving.

7. **General Tips**
   - Use the escape key to interrupt and redirect tasks.
   - For long prompts, use markdown files and refer to them in Claude using the @ symbol for clarity and organization.

These instructions should help you effectively manage and utilize Claude Code for coding projects. If you need more detailed guidance on any specific point, feel free to ask!

I'll search for files related to Claude's code instructions.Based on the search results, I can provide some key insights about Claude's code instructions:

Key Claude Code Instructions:

1. To-Do Lists
- Claude Code creates to-do lists automatically before writing code
- As Claude works, it checks off items in the to-do list
- Helps manage complex tasks without getting stuck in loops [[1]][c1]

2. Bash Mode
- Enables running bash commands directly inside Claude Code
- Can read/write files, search, and handle git commands
- Allows running commands without exiting the tool [[1]][c1]

3. Model Switching
- Can switch between different Anthropic models:
  - Opus: Most powerful model for complex challenges
  - Sonnet: Cost-efficient model for routine tasks
- Recommended modes:
  - Default: Use Opus until hitting 50% monthly usage
  - Opus Plan Mode: Use Opus for planning, Sonnet for implementation [[1]][c1]

4. claude.md File
- Project's "memory" file that gets added to context for every task
- Contains project-specific rules, workflows, and preferences
- Helps Claude follow specific project guidelines automatically
- Can be generated or updated by Claude itself [[2]][c2]

5. Advanced Techniques
- Can use multiple Claude instances simultaneously
- Supports parallel subagents for exploring multiple solutions
- Allows interrupting Claude using the escape key
- Supports test-driven development (TDD) [[1]][c1]

Pro Tips:
- Use planning mode before coding
- Utilize the message queue for queuing tasks
- Can input long prompts via markdown files
- Take screenshots for debugging [[1]][c1]

[c1]: detail/0/1978089728653656064
[c2]: detail/0/1994777493109735424

Would you like me to elaborate on any of these points?

Let me search your MyMemo documents to see if there are any relevant files about GitHub and coding workflows.Based on the search results, here's how you can use GitHub with Claude Code:

1. GitHub Integration Setup [[1]][c1]
- Run the "/install GH actions" command from the Claude Code menu
- This enables deep integration with GitHub actions

2. GitHub Workflow Capabilities [[2]][c2]:
a) Issue Management
- Tag Claude in GitHub issues and pull requests
- Ask Claude to fix issues
- Claude can run GitHub actions to submit PRs or fixes automatically
- Claude can review PRs automatically as they come in

3. Git and GitHub Interactions [[3]][c3]:
- Creating pull requests
- Implementing one-shot fixes for code review comments
- Fixing failing builds or linter warnings
- Categorizing and triaging open issues

4. Recommended Workflow: Explore, Plan, Code, Commit [[4]][c4]
- Ask Claude to read relevant files
- Have Claude make a plan for approaching a problem
- Implement the solution in code
- Commit the result and create a pull request
- Optionally update READMEs or changelogs

5. Best Practices [[5]][c5]:
- Be specific in your instructions
- Use `/clear` between distinct tasks to reset context
- Consider using subagents for different project phases
- Create custom commands for repetitive tasks

Key Tip: Think like a product manager by providing Claude with clear context and constraints [[6]][c6].

[c1]: detail/0/1978089728653656064
[c2]: detail/0/1978089728653656064
[c3]: detail/0/1995523602836881408
[c4]: detail/0/1995523602836881408
[c5]: detail/0/1995491336786018304
[c6]: detail/0/1978089728653656064


