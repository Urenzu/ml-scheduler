#!/bin/bash
# Auto-commit script

# Get current branch name
BRANCH=$(git rev-parse --abbrev-ref HEAD)

git add .                              # Stage all changes
git commit -m "Auto-commit"           # Commit with fixed message
git push origin "$BRANCH" --force      # Push to current branch forcefully
