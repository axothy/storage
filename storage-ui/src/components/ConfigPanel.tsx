import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getConfig, patchConfig } from '../hooks/useApi';
import { useState } from 'react';

export default function ConfigPanel() {
    const qc = useQueryClient();
    const { data, isLoading } = useQuery({ queryKey: ['config'], queryFn: getConfig });

    /* локальное редактируемое состояние */
    const [form, setForm] = useState<any>({});
    const mut = useMutation({
        mutationFn: patchConfig,
        onSuccess: () => qc.invalidateQueries({ queryKey: ['config'] }),
    });

    if (isLoading) return <p>Loading…</p>;
    const cfg = data ?? {};

    const onSubmit = (e: React.FormEvent) => {
        e.preventDefault();
        mut.mutate(form);
    };

    const field = (key: string, label: string, type = 'number') => (
        <label className="flex flex-col gap-1">
            <span>{label}</span>
            <input
                className="bg-neutral-800 border border-neutral-600 rounded px-2 py-1"
                type={type}
                defaultValue={cfg[key]}
                onChange={(e) => setForm({ ...form, [key]: e.target.value })}
            />
        </label>
    );

    return (
        <form onSubmit={onSubmit} className="max-w-md space-y-4">
            <h3 className="text-xl font-semibold mb-4">Конфигурация</h3>
            {field('flushThresholdBytes', 'Flush threshold, bytes')}
            {field('bloomFilterFalsePositiveProbability', 'Bloom FPP', 'number')}
            {field('bloomFilterHashFunctionsCount', 'Bloom hash count')}

            <button
                className="px-4 py-2 rounded bg-blue-600 hover:bg-blue-700"
                disabled={mut.isPending}
            >
                {mut.isPending ? 'Saving…' : 'Save'}
            </button>
        </form>
    );
}
