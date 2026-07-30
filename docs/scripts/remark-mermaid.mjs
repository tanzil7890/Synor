import { visit } from 'unist-util-visit';

const escapeHtml = (value) =>
  value
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;');

export default function remarkMermaid() {
  return (tree) => {
    visit(tree, 'code', (node, index, parent) => {
      if (node.lang !== 'mermaid' || !parent || typeof index !== 'number') {
        return;
      }
      parent.children[index] = {
        type: 'html',
        value: [
          '<figure class="mermaid-figure">',
          `<pre class="mermaid" aria-label="Diagram">${escapeHtml(node.value)}</pre>`,
          '</figure>',
        ].join(''),
      };
    });
  };
}
