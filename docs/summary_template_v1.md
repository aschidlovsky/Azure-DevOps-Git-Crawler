# Summary Template (Version 2.0)

> Always mirror this template exactly. Replace the bracketed tokens with real values and delete the brackets. Keep section titles, indentation, bullet styles, dividers, and ordering unchanged. If a section has no content, include the header and note `None`. Do not deviate from the required numbering or headings.

```
Summary Template v2.0

------------------------------------------------------------
# Functional Requirements — `<ObjectName>`

## 1. Initialization Behavior
- <Describe exactly how the form/class initializes data, handles args().record()/parm(), and creates or filters datasources.>
- Snippet:
  ```
  <AX code excerpt>
  ```
- Explanation: <Line-by-line description referencing specific calls, assignments, and conditions.>

## 2. Core Interaction Logic
- <Detail user-triggered events (button clicks, field modifications, selection changes) and helper class calls. Explain how datasources refresh after changes.>
- Snippet:
  ```
  <AX code excerpt>
  ```
- Explanation: <Factual description tied to concrete statements.>

## 3. Data Update & Persistence Behavior
- <Describe insert/update/delete sequences, update_recordset usage, ttsBegin/ttsCommit, recalculations, and when PO or ACK records are saved.>
- Snippet:
  ```
  <AX code excerpt>
  ```
- Explanation: <Call out each persistence statement and why it executes.>

## 4. Status / State Behavior (if applicable)
- <List every condition that locks fields, disables editing, or toggles actions. Include status values and their effects.>
- Snippet:
  ```
  <AX code excerpt>
  ```
- Explanation: <Describe the gating logic exactly as implemented.>

## 5. Filtering / Query Behavior (if applicable)
- <Explain executeQuery overrides, dynamic ranges, exists joins, and how user selections change list results.>
- Snippet:
  ```
  <AX code excerpt>
  ```
- Explanation: <Describe the precise query modifications and range values.>

## 6. Aggregate, Total, or Summary Calculations (if applicable)
- <Document total/summary functions, noting whether they iterate the datasource in-memory or issue queries.>
- Snippet:
  ```
  <AX code excerpt>
  ```
- Explanation: <Describe exactly how totals are computed.>

------------------------------------------------------------
# UI Requirements

- Describe editable vs. read-only controls, enablement rules, recalculation triggers, and conditional visibility without proposing redesigns.

| Control | Behavior |
|---------|----------|
| `<ControlName>` | `<Explain AllowEdit rules, enablement logic, and bound datasources/events>` |

------------------------------------------------------------
# Dependencies

## Business & Custom Application Objects
- `<ObjectName>` — `<Type>` — `<Purpose pulled directly from code usage>`

## Standard Application Objects (Not Kernel)
- `<ObjectName>` — `<Type>` — `<Purpose>`

## Filtered Out (Kernel / Framework / Runtime)
- `<ObjectName>` — `Excluded because <reason>`

------------------------------------------------------------
# Data Dictionary

## `<TableName>`
| Field | Description (based only on usage in this code) |
|-------|------------------------------------------------|
| `<FieldName>` | `<Explain how the code reads/sets/filters this field>` |

<Repeat for each referenced table.>
```
