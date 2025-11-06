# Summary Template (Version 1.1)

> Always mirror this template exactly. Replace the bracketed tokens with real values and delete the brackets. Keep section titles, indentation, bullet styles, and ordering unchanged. If a section has no content, include the header and note `None`.

```
Summary Template v1.1

Entry File
- Path: <path>
- Ref: <ref>
- SHA: <sha>
- Purpose: <plain-language description>

Entry Points & Triggers
1. <Context :: method>
   - Trigger context: <how it is invoked>
   - Purpose: <concise plain-language detail>
   - Functional impact: <requirement/workflow supported>
   - Snippet:
     ```
     <AX code excerpt>
     ```
   - Explanation: <describe exactly what the snippet does>
   - Key statements:
     - Line <line> — `<code>` → <explain the invoked call, assignment, or condition and the data it touches>
     - <Repeat for every critical statement captured in calls/assignments/conditions>
2. <Repeat for every method>

UI Highlights
- Control: <Name :: Type>
  - Purpose: <what the user can do>
  - Functional impact: <resulting behavior / downstream logic>
  - Snippet:
    ```
    <binding or handler code>
    ```
  - Explanation: <how the UI requirement is satisfied>
  - Key bindings:
    - Line <line> — `<property = value>` → <explain how this property drives the UI/data>
    - <Repeat for each relevant property>
... (one bullet per control)
- Datasource: <Name> — <brief UI role> (list each datasource not already covered)

Narrative Walkthrough
1. <Step 1 description> — Snippet:
   ```
   <AX code excerpt>
   ```
   Explanation: <plain-language detail>
2. <Continue steps until workflow complete>

Functional Requirements
1. <Requirement statement>
   - Supported by: <method(s)/operations>
   - Snippet:
     ```
     <AX code excerpt>
     ```
   - Explanation: <how the snippet enforces the requirement>
   - Implementation detail:
     - Line <line> — `<code>` → <describe the exact call/assignment/condition enforcing the requirement>
     - <Add additional lines as needed>
2. <Repeat for every requirement>

Functional Deep Dive
- Topic: <name>
  - Details: <plain-language description>
  - Snippet:
    ```
    <AX code excerpt>
    ```
  - Explanation: <why this detail matters>
- Topic: ...

CRUD Hooks
Explicit Overrides
1. <Context :: method>
   - Trigger context: <when/how it fires>
   - Purpose: <logic performed>
   - Functional impact: <requirement/workflow>
   - Snippet:
     ```
     <AX code excerpt>
     ```
   - Explanation: <plain-language summary>
2. <Repeat for every explicit override>

Implicit Operations
1. <Method :: operation (e.g., FormRun initAck → mssBDAckLine.insert)>
   - Trigger context: <method/event>
   - Purpose: <why data is written>
   - Functional impact: <requirement/workflow>
   - Snippet:
     ```
     <Actual insert/update/delete statement>
     ```
   - Explanation: <plain-language summary>
2. <Repeat for every implicit operation>

Business Rules
- Rule: <description> — Snippet:
  ```
  <AX code excerpt>
  ```
  Explanation: <how the rule works>
- Rule: ...

Branch Plan
Dependency                         Status              Notes
------------------------------------------------------------
<path>                             <status>            <short note>
<Repeat row for each dependency>

Branch Status Tracker
Dependency/Sub-Dependency           Status               Notes
----------------------------------------------------------------
<path>                              <status>             <note>
<Repeat row for each tracked dependency>
```
