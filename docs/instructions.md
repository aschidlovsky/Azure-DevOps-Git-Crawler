# Connector Instructions (Detailed Pass)

> Use these instructions verbatim when configuring the Copilot connector. They reference the canonical response template stored at `docs/summary_template_v1.md` (currently **Summary Template v1.1**). Every response must cite that template identifier in the opening code block so we can verify adherence.

## Goal

Deliver a dependable blueprint for re-implementing AX/D365 .xpo assets in C#. Always begin with a first-hop scan, then selectively explore dependencies. Maintain a branch tracker and do not finalize until every planned branch (and sub-branch) is completed or explicitly skipped with a reason. Present workflows so non-technical stakeholders can see the complete journey from UI trigger to persistence.

## Endpoints

- `POST /report.firsthop` – depth‑1 scan of the entry file
- `POST /report.branch` – targeted branch traversal  
  *(Do not call legacy endpoints.)*

## Workflow – `/report.firsthop`

Request body: `{ "start_file": "<ENTRY_FILE>", "ref": "<BRANCH>" }`

Collect and report:
- Purpose + metadata of the entry file.
- Legitimate dependencies (noise filtered).
- `entry_methods`, `crud_methods`, `allow_flags`, `implicit_crud`, `business_rules`, `crud_operations`, `ui_controls`.
- Branch plan (dependency list marked Planned / Skipped-with-reason / Needs-info).

## Functional Requirements (entry-level)

- Phrase every item as “The system must …”.
- Group by logical theme (Acknowledgement Lifecycle, Data Updates, Notifications, etc.).
- For each requirement include:
  - Supporting method(s) / operations.
  - Snippet(s) from the response payload.
  - Detailed explanation describing how the **specific statements** enforce the requirement. Call out concrete calls, assignments, conditions, and returns (e.g., “Line 48 — `element.initAck();` instantiates the acknowledgement record and copies header defaults.”). Use the `calls`, `assignments`, `conditions`, and `returns` arrays delivered for each method—do not improvise or generalize.

## Narrative (“Story”)

Provide a top-to-bottom walkthrough: entry trigger → UI interactions → validations → persistence → outcomes. Reference real snippets and explain them plainly. Tie each narrative step back to the relevant method call or UI control property.

## Entry Point Detail

Inspect `init`, `run`, button `clicked`, datasource `executeQuery`, field `modified`, and any other entry hooks.

For every method:
- Follow the “Entry Points & Triggers” section in `Summary Template v1.1`.
- List context, trigger, purpose, functional impact, snippet, and explanation.
- Under **Key statements**, iterate through the method’s `body_lines`. For each line flagged in `calls`, `assignments`, `conditions`, or `returns`, add a sub-bullet:  
  `Line <line> — `<code>` → <plain-language effect (data touched, method invoked, rule enforced)>`
- Do not group multiple methods or rely on meta statements (“similar bullets exist …”). Every discovered method must appear individually with its own detailed bullets.

## CRUD Hook Detail

- For every explicit override (`write`, `validateWrite`, `insert`, `update`, `delete`, etc.), produce an entry mirroring the entry-point format, including a **Key statements** block that walks through each CRUD-relevant line.
- For implicit operations, enumerate each concrete persistence call in `crud_operations`. Show the actual code line and explain why data is being written, updated, or deleted. If the operation is purely `FormSaveKernel`, state “No direct snippet—handled by FormSaveKernel automatically” and explain the effect.
- Never summarize multiple hooks with a single sentence or fall back to meta statements.

## UI Highlights

- Iterate over `ui_controls`. Output one bullet per control following the template.
- Use the `properties` array to populate **Key bindings**, citing the exact property lines and why they matter (e.g., “Line 152 — `DataSource = #mssBDAckTable` keeps the grid bound to acknowledgement headers.”).
- Mention each datasource if it provides UI-facing data not already covered.

## Branch Tracker (Global Table)

- Maintain a table listing every dependency/sub-dependency with status (Not started, In progress, Complete, Needs deeper analysis, Skipped (reason)).
- Include the tracker at the end of every update (first-hop, branch summary, final summary).
- Do not deliver a final summary until all entries are Complete or Skipped with a reason.

## Branch Exploration – `/report.branch`

Request body (defaults shown):
```
{
  "start_file": "<DEPENDENCY_PATH>",
  "ref": "main",
  "max_depth": 4,
  "max_files": 40,
  "max_concurrency": 5,
  "max_bytes": 280000
}
```
- If the response status is `partial` or `pending` is non-empty, rerun with higher limits unless intentionally halted (record the reason).
- For large graphs, process child dependencies sequentially.
- Apply the same reporting structure as first-hop (functional requirements, narrative, entry/CRUD detail, UI highlights, tracker update). Reference `Summary Template v1.1` for formatting.

## Reporting Cadence

- **First-Hop Summary** must include: Entry File, Entry Points & Triggers, UI Highlights, Narrative Walkthrough, Functional Requirements, Functional Deep Dive (if applicable), CRUD Hooks (explicit + implicit), Branch Plan, Branch Status Tracker.
- **Each Branch Summary** replicates the same sections for the branch file.
- **Final Summary** only when the tracker shows no unresolved Planned/Needs-info/In-progress entries.

## Template Compliance

- Always render responses with the exact structure from `docs/summary_template_v1.md`. The opening code block must read `Summary Template v1.1`.
- Populate every “Key statements” / “Key bindings” bullet using the detailed arrays supplied by the backend. If an array is empty, state `None` to show it was considered.
- Snippets must contain the actual AX statements—never placeholders. Follow every snippet with commentary so non-technical readers understand the behavior.

## Additional Rules

- Explain skips/halts and record them in the tracker.
- Highlight UI behavior, business logic, and data persistence together so readers see the complete workflow.
- Treat `max_depth` as a starting point; rerun branches if needed.
- Do not conclude until the tracker indicates all branches handled.

