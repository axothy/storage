import { useState } from 'react';
import { scan } from '../hooks/useApi';
import { useQuery } from '@tanstack/react-query';
import { Base64 } from 'js-base64';

export default function DataBrowser() {
    const [from, setFrom] = useState('');
    const [to, setTo] = useState('');
    const { data, refetch, isFetching } = useQuery({
        queryKey: ['scan', from, to],
        queryFn: () => scan(b64(from), b64(to)),
        enabled: false,
    });

    function b64(s: string) {
        return Base64.encodeURI(s);
    }

    return (
        <div className="space-y-4">
            <div className="flex gap-3 items-end">
                <Input label="From key" value={from} onChange={setFrom} />
                <Input label="To key" value={to} onChange={setTo} />
                <button
                    onClick={() => refetch()}
                    className="px-4 py-2 rounded bg-green-600 hover:bg-green-700"
                >
                    {isFetching ? 'Loading…' : 'Scan'}
                </button>
            </div>

            <table className="w-full border-collapse text-sm">
                <thead className="bg-neutral-800">
                <tr>
                    <th className="border px-2 py-1 text-left">Key</th>
                    <th className="border px-2 py-1 text-left">Value</th>
                </tr>
                </thead>
                <tbody>
                {data?.map((e: any) => (
                    <tr key={e.key}>
                        <td className="border px-2 py-1 font-mono">{e.key}</td>
                        <td className="border px-2 py-1 font-mono">{e.value}</td>
                    </tr>
                )) || (
                    <tr>
                        <td colSpan={2} className="text-center py-4">
                            No data
                        </td>
                    </tr>
                )}
                </tbody>
            </table>
        </div>
    );
}

function Input({
                   label,
                   value,
                   onChange,
               }: {
    label: string;
    value: string;
    onChange: (v: string) => void;
}) {
    return (
        <label className="flex flex-col gap-1 flex-1">
            <span>{label}</span>
            <input
                className="bg-neutral-800 border border-neutral-600 rounded px-2 py-1"
                value={value}
                onChange={(e) => onChange(e.target.value)}
            />
        </label>
    );
}
