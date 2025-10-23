# Git Workflow Best Practices for Maruko Project

## Preventing Aggressive Changes

This document outlines practices to avoid problematic changes that could be difficult to revert.

### 1. Branch Protection Strategy

Always work on feature branches instead of directly on master or develop:

```bash
# Create a feature branch for your work
git checkout -b feature/your-feature-name

# Or use the alias:
git feature descriptive-name
```

### 2. Regular Backup Strategy

Before making significant changes, create a backup:

```bash
# Stash current work with a descriptive name
git stash push -m "backup-before-major-refactor"

# Or use the alias:
git stash-safe
```

To restore a backup:
```bash
git stash list  # List all stashes
git stash pop   # Apply and remove the most recent stash
```

### 3. Pre-Commit Safety Checks

The project includes a pre-commit hook that will:
- Prevent commits directly to master branch
- Check for large files (>5MB) that shouldn't be tracked in git
- Run basic validation before allowing commits

### 4. Code Review Process

Always follow this process:
1. Complete your feature on a feature branch
2. Rebase on the latest develop branch: `git rebase-develop`
3. Submit a pull request for review
4. Have another team member review the changes
5. Merge only after approval

### 5. Recovery from Aggressive Changes

If you've made aggressive changes that need to be reverted:

```bash
# If the changes are only local and not committed:
git reset --hard HEAD

# If changes are committed but not pushed:
git reset --hard HEAD~n  # where n is number of commits to go back

# If changes are already pushed:
git revert HEAD  # Creates a new commit that undoes the changes
```

### 6. Useful Commands

The project includes several helpful aliases:

- `git st` - Show git status
- `git feature <name>` - Create a new feature branch with timestamp
- `git pull-develop` - Update your local develop branch
- `git rebase-develop` - Rebase current branch on develop
- `git lg` - Show a graphical log of commits
- `git stash-safe` - Stash with timestamp for easy identification
- `git changes` - Show changes compared to develop branch

### 7. Emergency Recovery

If you accidentally make aggressive changes to master or develop:

```bash
# Check current branch
git branch

# If on master/develop and shouldn't be:
git stash  # Save current changes temporarily
git checkout -b emergency-fix-branch  # Create new branch with changes
git checkout develop  # Go back to safe branch
git checkout -  # Switch back to emergency branch
```

### 8. Before Major Changes

Before making aggressive changes:

1. **Create a backup stash**: `git stash-safe`
2. **Create a feature branch**: `git feature before-aggressive-change`
3. **Pull latest changes**: `git pull-develop`
4. **Consider if the change is really needed** - can it be done in smaller increments?
5. **Test your current state** before making changes

### 9. Working with WIP (Work In Progress)

If you need to save work in progress safely:

```bash
# Save with descriptive message
git add .
git commit -m "wip: checkpoint before major change"

# Or use stash for temporary safekeeping
git stash-safe
```

This approach allows you to return to a known state if needed while keeping your work safe.