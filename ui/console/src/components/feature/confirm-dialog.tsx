import * as Dialog from '@radix-ui/react-dialog';
import { useState, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';

export interface ConfirmDialogProps {
  /** Resource name the user must re-type to enable the destructive button. */
  expectedToken: string;
  title: string;
  description: ReactNode;
  onConfirm: () => void | Promise<void>;
  trigger: ReactNode;
  danger?: boolean;
}

/**
 * Destructive-action confirm dialog (per ui-design-spec §5.3 + §5.6).
 *
 * The user must retype `expectedToken` (typically the resource name) to
 * enable the confirm button. No implicit capability check — wrap the
 * trigger in `<Can>` if the action is capability-gated.
 */
export function ConfirmDialog({
  expectedToken,
  title,
  description,
  onConfirm,
  trigger,
  danger = false,
}: ConfirmDialogProps) {
  const { t } = useTranslation();
  const [typed, setTyped] = useState('');
  const [open, setOpen] = useState(false);
  const [submitting, setSubmitting] = useState(false);

  const matches = typed === expectedToken;

  async function handleConfirm() {
    if (!matches) return;
    try {
      setSubmitting(true);
      await onConfirm();
      setOpen(false);
      setTyped('');
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <Dialog.Root open={open} onOpenChange={setOpen}>
      <Dialog.Trigger asChild>{trigger}</Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 bg-bg-overlay" />
        <Dialog.Content className="fixed left-1/2 top-1/2 w-[min(480px,90vw)] -translate-x-1/2 -translate-y-1/2 rounded-lg border border-border bg-bg-elevated p-6 shadow-lg">
          <Dialog.Title className="text-lg font-semibold">{title}</Dialog.Title>
          <Dialog.Description asChild>
            <div className="mt-2 text-sm text-fg-muted">{description}</div>
          </Dialog.Description>

          <label className="mt-4 block text-sm">
            <span className="mb-1 block text-fg-muted">
              Type <code className="px-1 bg-bg-subtle rounded">{expectedToken}</code>{' '}
              to confirm
            </span>
            <input
              className="w-full rounded-md border border-border bg-bg-base px-3 py-2 font-mono text-sm focus:outline-none focus:ring-2 focus:ring-border-focus"
              autoFocus
              value={typed}
              onChange={(e) => setTyped(e.target.value)}
              aria-label="confirm token"
            />
          </label>

          <div className="mt-6 flex justify-end gap-2">
            <Button variant="ghost" onClick={() => setOpen(false)}>
              {t('common.cancel')}
            </Button>
            <Button
              variant={danger ? 'danger' : 'default'}
              disabled={!matches || submitting}
              onClick={() => {
                void handleConfirm();
              }}
            >
              {t('common.confirm')}
            </Button>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
