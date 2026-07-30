# CLI Test Examples

Test examples for CLI commands: `ls`, `update`, `show`, and `drop`.

## Automated Tests

Run the automated CLI tests with:

```bash
pytest python/tests/cli/test_cli.py -v
```

The tests in `test_cli.py` use subprocess to run CLI commands and verify outputs.

## Manual Test Instructions

The sections below document manual test scenarios for reference.

## Test Setup

```bash
cd python/tests/cli

# Clean up all test artifacts before starting
rm -rf synor*.db db1 db2 db_alpha out_* __pycache__
```

## Test Cases

### 1. No Apps Defined (Edge Case)

Tests the error message when a module has no apps.

```bash
# Should show: "No apps are defined in './no_apps.py'."
synor ls ./no_apps.py

# Should show error: "No apps found after loading..."
synor update ./no_apps.py
```

### 2. Single App (Auto-Select)

Tests that a single app is automatically selected without needing `:app_name`.

```bash
# List - should show SingleApp with [+] indicator
synor ls ./single_app.py

# Update without specifying app name - should auto-select
synor update ./single_app.py

# Verify it ran
ls out_single/
# Expected: single.txt

# List again - no [+] indicator
synor ls ./single_app.py

# Clean up
synor drop ./single_app.py -f
```

### 3. Multiple Apps (Requires Specifier)

Tests that multiple apps require explicit `:app_name` specifier.

```bash
# List - should show both apps grouped under their db path
synor ls ./multi_app.py

# Update without specifier - should ERROR with "Multiple apps found"
synor update ./multi_app.py
# Expected error: "Multiple apps found in './multi_app.py': MultiApp1, MultiApp2..."

# Update with explicit app name
synor update ./multi_app.py:MultiApp1
synor update ./multi_app.py:MultiApp2

# Verify target states
ls out_multi_1/
ls out_multi_2/

# List - both should be persisted now
synor ls ./multi_app.py

# Remove one
synor drop ./multi_app.py:MultiApp1 -f

# List - MultiApp1 should show [+], MultiApp2 should not
synor ls ./multi_app.py

# Clean up
synor drop ./multi_app.py:MultiApp2 -f
```

### 4. App NOT Bound to Module-Level Variable (WeakValueDictionary Test)

Tests that apps created via factory functions (not bound to obvious module attributes) are still discoverable.

```bash
# List - should find UnboundApp even though it's created via factory
synor ls ./app_not_bound.py

# Update - should work
synor update ./app_not_bound.py

# Verify target state
ls out_unbound/

# Clean up
synor drop ./app_not_bound.py -f
```

### 5. Multiple Environments (Different Databases, Same Filename)

Tests apps in different environments are grouped correctly. Both use `synor.db` but in different directories.

```bash
# List - should show TWO groups (two different db paths, both ending in synor.db)
synor ls ./multi_env.py
# Expected output:
#   ./db1/synor.db:
#     DB1App [+]
#
#   ./db2/synor.db:
#     DB2App [+]

# Update both
synor update ./multi_env.py:DB1App
synor update ./multi_env.py:DB2App

# List again - should show both without [+]
synor ls ./multi_env.py

# Verify target states in different directories
ls out_db1/
ls out_db2/

# Clean up
synor drop ./multi_env.py:DB1App -f
synor drop ./multi_env.py:DB2App -f
rm -rf db1 db2
```

### 6. Same App Name in Different Environments

Tests that apps with the same name can coexist in different environments using the `@env_name` syntax. One app uses a named environment ("alpha"), and the other uses the default environment ("default").

```bash
# List - should show TWO groups, both with "MyApp" and their environment names
synor ls ./same_name_diff_env.py
# Expected output:
#   alpha (./db_alpha/synor.db):
#     MyApp [+]
#
#   default (./synor.db):
#     MyApp [+]

# Update without env specifier - should ERROR
synor update ./same_name_diff_env.py:MyApp
# Expected error: "Multiple apps named 'MyApp' found in different environments..."

# Update with @env_name specifier
synor update ./same_name_diff_env.py:MyApp@alpha
synor update ./same_name_diff_env.py:MyApp@default

# Verify target states in different directories
ls out_alpha/
ls out_default/

# List - both should be persisted now
synor ls ./same_name_diff_env.py

# Drop one environment's app
synor drop ./same_name_diff_env.py:MyApp@alpha -f

# List - alpha should show [+], default should not
synor ls ./same_name_diff_env.py

# Clean up
synor drop ./same_name_diff_env.py:MyApp@default -f
rm -rf db_alpha
```

### 7. Invalid App Name (Error Handling)

```bash
# Should error: "No app named 'NonExistent' found..."
synor update ./single_app.py:NonExistent
```

### 8. List from Database (--db option)

```bash
# First, run an app to persist it
synor update ./app1.py

# List using --db option (no module needed)
synor ls --db ./synor.db
# Expected: Shows TestApp1

# Clean up
synor drop ./app1.py -f
```

## Full Cleanup

```bash
rm -rf synor*.db db1 db2 db_alpha out_* __pycache__
```
