# Synor identity system

This system gives Synor a recognizable voice and visual language without
depending on an external brand package or hosted font service.

## Product promise

**Synor keeps every derived file, row, and index aligned with the inputs that
produced it.**

The short campaign line is **Built for the second run.** The promise explains
the outcome. The campaign line names the moment when Synor becomes useful:
after an input, function, or expected output changes.

## Identity idea

The Synor mark is called the **Alignment Weave**. Three paths enter from the
left. The upper and lower paths bend toward a stable center while all three
leave in alignment. It represents independent changes settling into a
coherent result.

The mark is not a letterform, hexagon, database cylinder, or recycling arrow.
It should never be redrawn as one.

### Logo files

- `public/images/synor-mark.svg`: square product mark and favicon.
- `public/images/synor-wordmark.svg`: horizontal lockup for wide placements.

Keep clear space equal to one path stroke around the square mark. Use the mark
at 24 CSS pixels or larger. Use the wordmark at 120 CSS pixels or larger.
Do not add gradients, shadows, outlines, or alternate path colors.

## Color

| Token | Value | Job |
|---|---:|---|
| Ink | `#19151F` | Primary text, dark surfaces, logo field |
| Canvas | `#F7F3EA` | Page background |
| Surface | `#FFFDF8` | Cards and reading surfaces |
| Spark | `#ED5A3A` | Action, active flow, first weave path |
| Signal | `#6F45E8` | Links, focus, selected state |
| Sprout | `#C6E85B` | Successful reuse, stable state |
| Lilac wash | `#EEE5FF` | Soft emphasis and code annotations |
| Blush | `#FFD1C7` | Removed or invalid state |
| Amber | `#F5C451` | Updated state and caution |

Ink on Canvas is the default pair. White text may sit on Ink, Signal Deep, or
Spark Deep. Spark and Sprout are accents, not body-text colors.

## Typography

Synor uses local system fonts, so documentation never makes a font request.

- **Display:** Iowan Old Style, then Palatino or Georgia. Use it for product
  headlines and major page titles.
- **Body:** Avenir Next, then Segoe UI, Helvetica, or Arial.
- **Code:** SFMono Regular, then Consolas or Liberation Mono.

Display text is compact and sentence-cased. Body copy uses a maximum measure
of 72 characters. Code remains visually separate on the Ink surface.

## Shape and motion

- Large surfaces use a 20 pixel radius.
- Controls and code panels use a 12 pixel radius.
- Data in diagrams is square. Work is rounded. Declared outcomes use a flat
  leading edge and a rounded trailing edge.
- Motion is quiet until hover. Diagram flow may drift on hover, and status
  marks may animate. Reduced-motion settings always win.

## Voice

Write like an engineer describing observed behavior to another engineer.
Lead with the concrete result, then explain the mechanism.

Preferred vocabulary:

| Prefer | Use when |
|---|---|
| input | Speaking broadly about data that can change |
| outcome | Marketing-level name for a managed file, row, or index entry |
| target state | Referring to the exact Synor API concept |
| work unit | Introducing a processing component in plain language |
| reconcile | Describing create, update, and removal as one operation |
| settled work | Work that memoization can safely reuse |
| second run | Explaining the value of incremental execution |

Avoid framework analogies, equations as slogans, “magic,” “effortless,”
“ultra-performant,” and claims that are not backed by a test or benchmark.

## Example strategy

Lead with examples that expose Synor’s behavior before adding infrastructure:

1. A local note catalog that turns Markdown notes into JSON records.
2. A file mirror that removes outputs when their inputs disappear.
3. A row-backed catalog where one edited record updates one outcome.
4. Vector and graph examples only after the local execution model is clear.

Every introductory example should answer three questions:

1. What changed?
2. What work ran?
3. What outcome was repaired?

## Accessibility

- Normal text must meet WCAG AA contrast.
- Focus uses a visible Signal outline at least 2 pixels wide.
- Color never carries state alone. Labels, shape, or icons repeat the meaning.
- Logo SVGs include accessible titles when used as content. Decorative uses
  must be hidden from assistive technology.
- All motion has a reduced-motion fallback.
