import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';

export function LangToggle() {
  const { i18n, t } = useTranslation();
  const current = i18n.language.startsWith('zh') ? 'zh-CN' : 'en';
  const next = current === 'zh-CN' ? 'en' : 'zh-CN';
  return (
    <Button
      size="sm"
      variant="outline"
      onClick={() => {
        void i18n.changeLanguage(next);
      }}
    >
      {current === 'zh-CN' ? t('lang.en') : t('lang.zh')}
    </Button>
  );
}
