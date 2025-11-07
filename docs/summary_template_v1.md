# Summary Template (Version 3.0)

> Always mirror this structure exactly. Replace bracketed tokens with real values and delete the brackets. Keep section titles, numbering, indentation, bullet styles, and ordering unchanged. If a section does not apply, include the header and note `None`. Every numbered requirement must cite at least one snippet directly from the provided AX source.

```
Summary Template v3.0

# **Functional Requirements — `<ObjectName>`**

## 1. Form Initialization

1.1 <When/condition statement (e.g., “When the form opens with PurchTable context”)>
- Requirement: `<The system must …>`
- Snippet:
  ```
  <AX code excerpt>
  ```
- Explanation: `<Plain-language explanation tied to the snippet, citing fields/methods>`

1.2 <Next condition> … (create as many numbered sub-items as needed)

## 2. Line-Level Matching / Interaction Logic

2.1 <Requirement statement>  
- Requirement: `<The system must …>`
- Snippet:
  ```
  <AX code excerpt>
  ```
- Explanation: `<Plain-language translation>`

2.2 <Additional behaviors> …

## 3. Planned Ship Date / Auxiliary Updates (rename if needed)

3.1 … (follow the same bullet/snippet/explanation pattern)

## 4. Ship-To / Address Handling (rename to match the object)

4.1 …

## 5. Filtering / Query Behavior

5.1 …

## 6. Posting / Completion Logic

6.1 …

## 7. Accepting Prices / Secondary Actions

7.1 …

## 8. Bulk Match Operations (MatchAll, etc.)

8.1 …

## 9. Void / Cancel Behavior

9.1 …

## 10. Read-Only / Disabled Editing Conditions

10.1 …

## 11. Totals / Aggregate Calculations

11.1 …

*(Add or remove sections 3–11 as necessary, but keep numbering sequential. Each item must include Requirement + Snippet + Explanation.)*

------------------------------------------------------------
# **UI Requirements**

## A. Header / Overview Controls
| Control | Behavior |
|---------|----------|
| `<ControlName>` | `<Explain AllowEdit, enablement, and handler references with snippet citations>` |

## B. Section-Specific Controls
| Control | Rules |
|---------|-------|
| `<ControlName>` | `<Describe triggers, enablement, and linked methods>` |

## C. Actions / Buttons
| Button | Enabled When | Behavior |
|--------|--------------|----------|
| `<ButtonName>` | `<Condition>` | `<Exact action performed (cite code)>` |

## D. Line Grid Behavior
| Field | Edit Rule |
|-------|-----------|
| `<FieldName>` | `<Rule derived from source>` |

------------------------------------------------------------
# **Dependencies**

## Business & Custom-Application Objects Used
| Object | Type | Purpose |
|--------|------|---------|
| `<Name>` | `<Table/Class/Enum>` | `<Purpose derived from usage>` |

## Standard Application Objects (Not Kernel — Retained)
| Object | Type | Purpose |
|--------|------|---------|
| `<Name>` | `<Standard table/class>` | `<Purpose>` |

## Filtered Out (Kernel / Framework / Runtime)
| Object | Reason Removed |
|--------|----------------|
| `<Name>` | `Excluded because <reason>` |

------------------------------------------------------------
# **Data Dictionary**

## `<TableName>`
| Field | Description (based only on code usage) |
|-------|----------------------------------------|
| `<FieldName>` | `<What the code reads/sets/filters>` |

<Repeat for each table referenced.>
```
