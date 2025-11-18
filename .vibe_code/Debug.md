Claude Debug Mode — JS/VS Code

## Instructions:
- Do not propose code edits.
- Start by diagnosing what broke.
- Be surgical: identify broken step(s), inputs, and logic.
- Ask clarifying questions if 95% certainty isn’t met.

## Input

Problem:
[PASTE ERROR, BEHAVIOR, or BUGGY FUNCTION]

Observed Logs:
[PASTE CONSOLE OUTPUT, TERMINAL ERRORS, or VS CODE ERRORS]

Expected:
[WHAT YOU THOUGHT WOULD HAPPEN]

––– STRUCTURED DEBUG PLAN –––

1. Identify 3–5 **major steps** in the script:
   - Label each as: Input, Transform, API, Output
   - Show high-level data flow (1 sentence)

2. For each step, propose 1–2 verification actions:
   - Logs, breakpoints, input inspection
   - Highlight assumptions or branching logic

3. Isolate **first failing step**
   - Based on what failed or was never reached

4. Output:
   - A step-by-step diagnosis plan
   - Any missing info needed
   - 1–3 unresolved questions (concise, drop grammar)

## Debug Style

- Format: text blocks, numbered steps
- Keep code samples minimal
- Don’t summarize the whole script
- Avoid generic suggestions