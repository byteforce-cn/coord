import type { Config } from 'tailwindcss';

/**
 * Tailwind theme wired to the CSS Variables defined in
 * `src/styles/tokens.css`. This keeps every color/spacing/radius value
 * in a single source of truth per `doc/ui-design-spec.md` §2.
 * Components MUST NOT hard-code colors or spacings.
 */
const config: Config = {
  darkMode: ['class', '[data-theme="dark"]'],
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        bg: {
          base: 'var(--color-bg-base)',
          subtle: 'var(--color-bg-subtle)',
          muted: 'var(--color-bg-muted)',
          elevated: 'var(--color-bg-elevated)',
          overlay: 'var(--color-bg-overlay)',
        },
        fg: {
          DEFAULT: 'var(--color-fg-default)',
          muted: 'var(--color-fg-muted)',
          subtle: 'var(--color-fg-subtle)',
          'on-accent': 'var(--color-fg-on-accent)',
        },
        border: {
          DEFAULT: 'var(--color-border-default)',
          strong: 'var(--color-border-strong)',
          focus: 'var(--color-border-focus)',
        },
        accent: {
          DEFAULT: 'var(--color-accent)',
          hover: 'var(--color-accent-hover)',
          subtle: 'var(--color-accent-subtle)',
        },
        success: {
          DEFAULT: 'var(--color-success)',
          subtle: 'var(--color-success-subtle)',
        },
        warning: {
          DEFAULT: 'var(--color-warning)',
          subtle: 'var(--color-warning-subtle)',
        },
        danger: {
          DEFAULT: 'var(--color-danger)',
          subtle: 'var(--color-danger-subtle)',
        },
        info: {
          DEFAULT: 'var(--color-info)',
          subtle: 'var(--color-info-subtle)',
        },
        domain: {
          cluster: 'var(--color-domain-cluster)',
          registry: 'var(--color-domain-registry)',
          config: 'var(--color-domain-config)',
          lock: 'var(--color-domain-lock)',
          workflow: 'var(--color-domain-workflow)',
          transit: 'var(--color-domain-transit)',
          pki: 'var(--color-domain-pki)',
          security: 'var(--color-domain-security)',
          backup: 'var(--color-domain-backup)',
        },
      },
      borderRadius: {
        sm: 'var(--radius-sm)',
        DEFAULT: 'var(--radius-md)',
        md: 'var(--radius-md)',
        lg: 'var(--radius-lg)',
        xl: 'var(--radius-xl)',
      },
      boxShadow: {
        sm: 'var(--shadow-sm)',
        DEFAULT: 'var(--shadow-md)',
        md: 'var(--shadow-md)',
        lg: 'var(--shadow-lg)',
      },
      fontFamily: {
        sans: ['var(--font-sans)'],
        mono: ['var(--font-mono)'],
      },
      fontSize: {
        xs: 'var(--text-xs)',
        sm: 'var(--text-sm)',
        base: 'var(--text-base)',
        lg: 'var(--text-lg)',
        xl: 'var(--text-xl)',
        '2xl': 'var(--text-2xl)',
        '3xl': 'var(--text-3xl)',
      },
    },
  },
  plugins: [],
};

export default config;
