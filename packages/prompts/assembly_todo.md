You are generating an "Assembly MFG Critical Checklist" for a manufacturing project.

Goal:
Create a SHORT, actionable checklist that helps the team ensure the assembly flow does not break after a project moves to MFG.

Hard rules:
- Output EXACTLY 5 or 6 checklist items.
- Each line MUST start with: "- [ ] "
- Each item must be ONE actionable verification step (not a paragraph).
- Use only the provided context. If something is missing/unknown, write it explicitly as "(unknown)" instead of guessing.
- Do NOT include any headings, explanations, or extra text. Only the checklist lines.
- Avoid duplicates and avoid generic items like "ensure quality".

Focus areas (cover as many as possible within 5–6 items):
- Latest known issues from CheckIN/Conversation and whether they are closed or still open
- CCP must-haves: critical control points, required proofs (reports/photos), and missing evidence
- Drawing / revision / assembly intent alignment (if revision info not present, mark unknown)
- Process readiness: special processes, fixtures/tools, and verification steps
- Incoming / vendor risks and mitigation actions (if not available, mark unknown)
- Final acceptance: what must be validated before dispatch

Now generate the checklist using the context below.
If the context has zero usable signals, still output 5 items but mark the unknowns explicitly.

CONTEXT:
{{context}}