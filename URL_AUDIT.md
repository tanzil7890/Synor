# Public URL and contact audit

Checked: 2026-07-27

## Result

- Project-controlled public URLs and email addresses were removed because
  control of those endpoints has not been demonstrated in this workspace.
- The documentation defaults to a localhost canonical URL. A public build must
  set `SYNOR_DOCS_SITE_URL` to an endpoint the publisher controls.
- No public support, security, Discord, or community contact is advertised.
  Those channels must be added only after ownership and monitoring are
  established.
- Email addresses contained in test fixtures, example input documents, RFCs,
  and research-paper metadata are sample or third-party source data; they are
  not represented as Synor contacts.

## Link verification

Documentation links were checked from the repository root with:

```bash
/Users/tanzil/.cargo/bin/lychee \
  --accept 200,203,301..=304,403,429 \
  --no-progress \
  --root-dir . \
  --exclude-path '(^|/)node_modules/' \
  --exclude-path '(^|/)dist/' \
  --exclude-path '(^|/)markdown_files/' \
  README.md CONTRIBUTING.md CODE_OF_CONDUCT.md ORIGIN.md NOTICE \
  BRAND_CLEARANCE.md CHANGES_FROM_UPSTREAM.md URL_AUDIT.md \
  docs/DESIGN_SYSTEM.md \
  'docs/**/*.md' 'docs/**/*.mdx' 'examples/**/*.md'
```

The check reported 605 links, 302 unique endpoints, 200 successful checks,
405 deliberately excluded links, 29 redirects, and zero errors. Generated
dependency trees, built output, and bundled third-party research/RFC documents
are excluded because their links are not Synor advertising. HTTP 403 and 429
responses are accepted because they indicate a reachable endpoint that
declined or rate-limited automated checking, not successful access to the
content.

This audit must be repeated immediately before publication. It verifies link
reachability, not ownership, endorsement, licensing, privacy, or trademark
rights.
