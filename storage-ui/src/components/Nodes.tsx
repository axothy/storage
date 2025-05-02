import { useQuery } from '@tanstack/react-query';
import { getNodes } from '../hooks/useApi';
import { CheckCircle2, Circle, AlertTriangle } from 'lucide-react';
import clsx from 'clsx';

export default function Nodes() {
    const { data, isLoading } = useQuery({ queryKey: ['nodes'], queryFn: getNodes, refetchInterval: 2000 });

    if (isLoading) return <p>Loading…</p>;

    return (
        <table className="w-full border-collapse text-sm">
            <thead className="bg-neutral-800">
            <tr>
                <th className="border px-2 py-1">ID</th>
                <th className="border px-2 py-1">Role</th>
                <th className="border px-2 py-1">Last HB, ms</th>
                <th className="border px-2 py-1">Addr</th>
            </tr>
            </thead>
            <tbody>
            {data?.map((n: any) => (
                <tr key={n.id} className={clsx({ 'bg-yellow-900/20': n.role === 'LEADER' })}>
                    <td className="border px-2 py-1 flex items-center gap-1">
                        {icon(n)} {n.id}
                    </td>
                    <td className="border px-2 py-1">{n.role}</td>
                    <td className="border px-2 py-1">{n.lastHeartbeatMs}</td>
                    <td className="border px-2 py-1">{n.address}</td>
                </tr>
            ))}
            </tbody>
        </table>
    );
}

function icon(n: any) {
    if (n.role === 'LEADER') return <CheckCircle2 size={14} className="text-green-400" />;
    if (n.lastHeartbeatMs > 5000) return <AlertTriangle size={14} className="text-red-400" />;
    return <Circle size={10} className="text-neutral-500" />;
}
