import { forceFlush, forceCompact } from '../hooks/useApi';
import { useMutation } from '@tanstack/react-query';
import { Loader2, Sparkles, Package } from 'lucide-react';

export default function Header() {
    const flush = useMutation({ mutationFn: forceFlush });
    const compact = useMutation({ mutationFn: forceCompact });

    return (
        <header className="h-14 border-b border-neutral-700 bg-neutral-800/60 backdrop-blur px-6 flex items-center gap-4">
            <h2 className="font-semibold text-lg flex-1">LSM Cluster UI</h2>

            <ActionButton
                label="Flush"
                icon={<Sparkles size={16} />}
                onClick={() => flush.mutate()}
                loading={flush.isPending}
            />
            <ActionButton
                label="Compact"
                icon={<Package size={16} />}
                onClick={() => compact.mutate()}
                loading={compact.isPending}
            />
        </header>
    );
}

function ActionButton({
                          label,
                          icon,
                          onClick,
                          loading,
                      }: {
    label: string;
    icon: React.ReactNode;
    onClick: () => void;
    loading: boolean;
}) {
    return (
        <button
            className="flex items-center gap-1 border px-3 py-1 rounded-md text-sm
                 hover:bg-neutral-700 disabled:opacity-60"
            onClick={onClick}
            disabled={loading}
        >
            {loading ? <Loader2 className="animate-spin" size={16} /> : icon}
            {label}
        </button>
    );
}
