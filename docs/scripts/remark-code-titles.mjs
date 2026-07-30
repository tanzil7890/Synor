import { visit } from 'unist-util-visit';

const TITLE_ATTRIBUTE = /\btitle\s*=\s*(?:"([^"]+)"|'([^']+)')/;

function escapeHtml(value) {
  return value
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');
}

function displayLabel(node) {
  const match = node.meta?.match(TITLE_ATTRIBUTE);
  if (match) {
    node.meta =
      node.meta.replace(TITLE_ATTRIBUTE, '').replace(/\s+/g, ' ').trim() || null;
    return (match[1] ?? match[2]).split('/').at(-1);
  }
  return node.lang || 'text';
}

/**
 * Give fenced code samples a small, accessible label without depending on a
 * separately licensed theme package.
 */
export default function remarkCodeTitles() {
  return (tree) => {
    visit(tree, 'code', (node, index, parent) => {
      if (!parent || typeof index !== 'number') return;

      const label = escapeHtml(displayLabel(node));
      parent.children.splice(
        index,
        1,
        {
          type: 'html',
          value: `<section class="code-panel"><div class="code-panel-label">${label}</div>`,
        },
        node,
        { type: 'html', value: '</section>' },
      );
      return index + 3;
    });
  };
}
