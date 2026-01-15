You are generating an "Assembly MFG Critical Checklist" for a manufacturing project.

Goal:
Create a SHORT, actionable checklist that helps the team ensure the assembly flow does not break after a project moves to MFG.

Hard rules:
- Output EXACTLY 5 or 6 checklist items.
- Each line MUST start with: "- [ ] "
- Each item must be ONE actionable verification step (not a paragraph).
- Use only the provided context. If something is missing/unknown, write it explicitly as "(unknown)" instead of guessing.
- Do NOT include any headings, explanations, or extra text. Only the checklist lines. But it should be self explanatory.
- Avoid duplicates and avoid generic items like "ensure quality".
- Refer from the previous context. Our main goal is to ensure that the User will not do mistake. So we need to check CCP. Process, RM, Boughtouts of that Assembly (ID). Also if similar kind of thing has happened in past.

Update behavior (IMPORTANT):
- If PREVIOUS_CHECKLIST is present, keep still-relevant open items.
- Remove items that are clearly already satisfied/closed per the latest context (mention closure evidence briefly in parentheses if needed).
- Add new items only if the context introduces new risks/requirements.

Focus areas (cover as many as possible within 5–6 items):
- Latest known issues from history + what actually worked (resolutions)
- CCP must-haves: critical control points, required proofs (reports/photos), and missing evidence
- Drawing / revision / assembly intent alignment (if revision info not present, mark unknown)
- Process readiness: special processes, fixtures/tools, and verification steps
- Incoming / vendor risks and mitigation actions (if not available, mark unknown)
- Final acceptance: what must be validated before dispatch

Now generate the checklist using the context below.

CONTEXT:
{{context}}