export const synorCodeTheme = {
  name: 'synor-night',
  type: 'dark',
  colors: {
    'editor.background': '#0b1220',
    'editor.foreground': '#dbe7f5',
  },
  tokenColors: [
    {
      scope: ['comment', 'punctuation.definition.comment'],
      settings: { foreground: '#8291a8', fontStyle: 'italic' },
    },
    {
      scope: ['keyword', 'storage', 'storage.type'],
      settings: { foreground: '#7dd3fc' },
    },
    {
      scope: ['entity.name.function', 'support.function', 'variable.function'],
      settings: { foreground: '#5eead4' },
    },
    {
      scope: ['string', 'string.quoted'],
      settings: { foreground: '#bef264' },
    },
    {
      scope: ['constant.numeric', 'constant.language'],
      settings: { foreground: '#fde68a' },
    },
    {
      scope: ['entity.name.type', 'entity.name.class', 'support.type'],
      settings: { foreground: '#c4b5fd' },
    },
    {
      scope: ['variable', 'variable.parameter'],
      settings: { foreground: '#e2e8f0' },
    },
  ],
};

export default synorCodeTheme;
