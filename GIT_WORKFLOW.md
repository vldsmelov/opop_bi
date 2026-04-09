# Git workflow

## Branches

- `main`: stable state.
- `develop`: integration branch for ongoing work.
- `feature/<short-name>`: task branches.

## Daily flow

1. Start from `develop`.
2. Create a feature branch:
   - `git switch develop`
   - `git pull --ff-only` (when remote appears)
   - `git switch -c feature/<short-name>`
3. Commit small logical steps:
   - `git add -A`
   - `git commit -m "feat: short description"`
4. Merge back:
   - `git switch develop`
   - `git merge --no-ff feature/<short-name>`
5. When ready to release:
   - merge `develop` -> `main`
   - tag if needed: `git tag vX.Y.Z`

## Useful recovery commands

- View history:
  - `git log --oneline --graph --decorate --all`
- Restore a file from last commit:
  - `git restore <path>`
- Revert a bad commit without rewriting history:
  - `git revert <commit>`
