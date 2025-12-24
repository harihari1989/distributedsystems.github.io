# AGENTS.md

## Project Overview
- This repo hosts a professional distributed-systems visual guide for GitHub Pages.
- Output must be web-ready HTML suitable for direct publication.
- All content must be original, accurate, and production-grade.

## Content Rules
- Always explain: Problem -> Pattern -> Trade-offs -> Failure modes.
- Avoid repetition across pages.
- Cross-link related concepts instead of redefining them.

## Formatting Rules
- Use HTML headings only (H1 to H3).
- Use tables for comparisons.
- Use Mermaid for static diagrams.
- Use React/D3 only for concepts that benefit from interaction.

## Diagram Rules (Mermaid)
- Use this format:

<pre class="mermaid">
sequenceDiagram
  Client->>Service: Request
  Service-->>Client: Response
</pre>

- Never use fenced Mermaid blocks alone.
- Assume Mermaid JS is globally initialized.

## Interactive Visuals
- D3.js / Plotly via CDN only.
- No server-side code.
- Use embedded <script> blocks.
- Data must be local JSON or inline.

## React Usage
- Functional components only.
- Self-contained.
- Mount via ReactDOM.render.
- No Node server assumptions.

## Dark / Light Mode Constraints
- Never hardcode colors.
- Use CSS variables only.
- Assume data-color-mode="light|dark" on <html>.

## Page Template
Each page should follow:
1. Title
2. Problem framing
3. Core idea / pattern
4. Architecture diagram
5. Step-by-step flow
6. Failure modes
7. Trade-offs
8. Real-world usage

## Maintaining Context Across Pages
- Refer to existing pages instead of redefining concepts.
- Assume a glossary exists for CAP, ACID, quorum, etc.
- Prefer links over duplication.

## Constraints
- Static-site only.
- No backend assumptions.
- No build tools unless explicitly requested.
- Leave TODO markers instead of guessing.

## End State Vision
- A visual system design textbook.
- A living reference for distributed architecture.
