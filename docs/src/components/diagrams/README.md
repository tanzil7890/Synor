# Synor diagram system

Synor diagrams are inline Astro components built from a small semantic shape
set. They use the Alignment Weave palette from `src/styles/tokens.css` and the
voice defined in `docs/DESIGN_SYSTEM.md`.

New diagrams start from a blank layout. Do not trace exported artwork, match an
upstream composition, or bring static design-tool SVGs back into the docs.

## Shape vocabulary

Shape communicates the technical role before color does.

| Shape | Meaning | Primitive |
|---|---|---|
| Sharp rectangle | Input or intermediate data | `DataBox` |
| Rounded rectangle | Logic, subsystem, or operation | `LogicBox` |
| Header container | Processing component and ownership boundary | `ProcessingComponent` |
| Large rounded field | Synor App and run boundary | `AppContainer` |
| Flat-left capsule | Declared target state | `TargetBullet` |
| Framed snapshot | One run or comparison panel | `RunPanel` |
| Dashed arrow | Causal flow | `FlowArrow` |
| Plain line | Binding or membership | `Connector` |

All stateful shapes compose `ShapeGroup`. It owns position, state classes,
highlighting, memo marks, and cache-status badges.

## Identity palette

Diagrams use only tokens from the docs design system.

| Token | Purpose |
|---|---|
| `--ink` | Text and dark detail |
| `--canvas` | Neutral data fill |
| `--surface` | Run panels |
| `--spark` | Active flow |
| `--signal` | Default outlines |
| `--sprout` | New, reused, or successful state |
| `--amber` | Updated state |
| `--blush` | Removed state |
| `--lilac-wash` | App and component tint |
| `--rule` | Quiet connectors and dividers |

Do not hardcode colors in a concept diagram. A one-off color belongs in
`tokens.css` first, with a documented job.

## State and cache annotations

Data, logic, components, and target bullets accept a `state`:

| State | Meaning |
|---|---|
| `idle` | No change in this run |
| `new` | Created in this run |
| `updated` | Changed in place |
| `removed` | No longer belongs |
| `changed` | Fingerprint propagation only |

Logic boxes and processing components can also be memoized. A
`cache-ready` badge means settled work was reused. A `refreshing` badge means
the function ran again.

State CSS uses a direct-child selector. This prevents a component’s state from
painting every nested shape.

## Current concept diagrams

```text
concepts/
├── SecondRun.astro          first pass compared with a changed second pass
├── ReconcileCycle.astro     observe, own, and reconcile around local history
├── OwnershipLedger.astro    stable work paths and owned outcomes
└── ParentChildCalls.astro   mount compared with use_mount
```

These diagrams are original Synor compositions. Keep their narrative tied to
the current documentation language.

## Layout rules

1. Put named dimensions and positions at the top of the file.
2. Derive container width and height from its contents.
3. Keep left and right padding equal. Keep top and bottom padding equal.
4. Prefer horizontal arrows when rows can align.
5. Use `Connector` for membership and `FlowArrow` for causation.
6. Use local coordinates inside `AppContainer`, `ProcessingComponent`, and
   `RunPanel`.
7. Render labels with the primitive’s `foreignObject`. Do not split labels by
   hand.

The docs column is narrow. Most diagrams should remain legible around 720 CSS
pixels.

## Motion and accessibility

Motion is idle until the user hovers over a diagram. Flow drift, delta pulses,
cache checks, and refresh indicators all stop under
`prefers-reduced-motion: reduce`.

Every `DiagramFrame` needs a useful `title` and `desc`. Color cannot be the only
state cue. Labels, status badges, shape, and strike-through treatment repeat
the meaning.

## Embedding

Import concept components from the site root:

```mdx
import SecondRun from '/src/components/diagrams/concepts/SecondRun.astro';

<SecondRun />
```

## Verification

Any coordinate, shape, or layout change must be rendered and inspected:

```bash
dev/agent-skills/synor-diagrams/scripts/preview.sh \
  programming_guide/core_concepts
```

The preview helper builds the site, serves it under the required `/docs` base
path, captures a headless-Chrome screenshot, and creates a crop. Inspect the
PNG before considering the diagram complete.
