---
trigger: always_on
---

## CRITICAL — Git Operations
- NEVER run `git commit`, `git push`, `git add && git commit`, or any git write operation.
- Always let the user decide when and how to commit.
- You may run read-only git commands like `git status`, `git diff`, `git log`, `git remote -v`.
