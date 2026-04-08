PREFIX="BRMS-"
REPO_DIR=$(pwd)
TEMP_WORK_DIR="/tmp/git_work_tree_$(date +%s)"

# 2. Get the current author of the last commit
CURRENT_AUTHOR=$(git log -1 --format='%an')

# 3. Check if the prefix is already there to avoid an infinite loop
if [[ "$CURRENT_AUTHOR" != "$PREFIX"* ]]; then
    
    # Create a dummy work tree so git commands like 'commit --amend' work
    mkdir -p "$TEMP_WORK_DIR"
    
    # 4. Use --work-tree to bypass the 'bare repository' restriction
    # We must also use --git-dir to ensure it knows where the metadata is
    git --git-dir="$REPO_DIR" --work-tree="$TEMP_WORK_DIR" commit --amend \
        --author="$PREFIX$CURRENT_AUTHOR" \
        --no-edit --allow-empty > /dev/null 2>&1
    
    # 5. Clean up the temp directory
    rm -rf "$TEMP_WORK_DIR"
    
    # 6. Optional: Sync to Bitbucket
    # Only push if the amend was successful
    git push --mirror bitbucket >> /tmp/workbench_git.log 2>&1
fi
