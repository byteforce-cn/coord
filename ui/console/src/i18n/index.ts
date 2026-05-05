import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';
import zh from './zh-CN.json';
import en from './en.json';

/**
 * i18n bootstrap. Default language is 简体中文 per product contract;
 * English is always loaded so the language switcher can toggle without
 * additional network fetches. Untranslated keys fall through to the
 * key itself in dev for quick visual diagnosis.
 */
void i18n.use(initReactI18next).init({
  resources: {
    'zh-CN': { translation: zh },
    en: { translation: en },
  },
  lng: 'zh-CN',
  fallbackLng: 'zh-CN',
  interpolation: { escapeValue: false },
  returnNull: false,
});

export { i18n };
