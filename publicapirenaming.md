2. Complete rename list
Old	New
syn.fn	syn.task
syn.mount	syn.spawn
syn.mount_each	syn.spawn_each
syn.use_mount	syn.call
syn.mount_target	syn.attach_target
syn.component_subpath	syn.unit_path
syn.ComponentSubpath	syn.UnitPath
syn.ComponentMountHandle	syn.SpawnHandle
syn.declare_target_state	syn.ensure_target_state
syn.declare_target_state_with_child	syn.ensure_target_state_with_child
syn.NON_EXISTENCE	syn.ABSENT
syn.NonExistenceType	syn.AbsentType
syn.is_non_existence	syn.is_absent
.declare_row	.ensure_row (8 connectors)
localfs.declare_file	localfs.ensure_file
localfs.declare_dir_target	localfs.ensure_dir_target
.declare_table_target	.ensure_table_target (9 connectors)
@syn.task(memo=True)	@syn.task(cache=True)
Deliberately kept: TargetState* family (your call), mount_kind + its "mount"/"mount_each" values (your call), all core.* Rust names, memo_key/memo_fingerprint/MemoStateOutcome, and Tier B generics (App, Environment, ContextKey, map).

