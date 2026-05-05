import { type ClassValue, clsx } from 'clsx';
import { twMerge } from 'tailwind-merge';

/**
 * Conditional class-name joiner used by shadcn-style components.
 * Prefer this helper over manual string concatenation so Tailwind
 * classes collapse correctly under variant overrides.
 */
export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}
