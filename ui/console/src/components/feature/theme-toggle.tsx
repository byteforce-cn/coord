import { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';

type Theme = 'light' | 'dark' | 'hc';

/**
 * Cycle light → dark → high-contrast. Persisted to localStorage only
 * for user convenience — no sensitive data.
 */
export function ThemeToggle() {
  const { t } = useTranslation();
  const [theme, setTheme] = useState<Theme>(() => {
    const stored = localStorage.getItem('coord.theme');
    return (stored as Theme) || 'light';
  });

  useEffect(() => {
    document.documentElement.dataset['theme'] = theme;
    localStorage.setItem('coord.theme', theme);
  }, [theme]);

  const next: Record<Theme, Theme> = { light: 'dark', dark: 'hc', hc: 'light' };
  const label: Record<Theme, string> = {
    light: t('theme.light'),
    dark: t('theme.dark'),
    hc: t('theme.hc'),
  };

  return (
    <Button size="sm" variant="outline" onClick={() => setTheme(next[theme])}>
      {label[theme]}
    </Button>
  );
}
