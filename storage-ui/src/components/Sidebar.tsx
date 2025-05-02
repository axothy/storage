import { NavLink } from 'react-router-dom';
import { Database, Settings, Server } from 'lucide-react';

export default function Sidebar() {
    return (
        <aside className="w-56 bg-neutral-800 p-4 space-y-4">
            <h1 className="text-xl font-bold mb-6">LSM UI</h1>
            <nav className="space-y-2">
                <Link to="/" icon={<Database size={18}/>}>Данные</Link>
                <Link to="/config" icon={<Settings size={18}/>}>Конфигурация</Link>
                <Link to="/nodes" icon={<Server size={18}/>}>Узлы</Link>
            </nav>
        </aside>
    );
}

function Link({ to, icon, children }: React.PropsWithChildren<{ to: string; icon: React.ReactNode }>) {
    return (
        <NavLink
            to={to}
            className={({ isActive }) =>
                'flex items-center gap-3 px-3 py-2 rounded-lg ' +
                (isActive ? 'bg-neutral-700' : 'hover:bg-neutral-800')
            }
        >
            {icon} <span>{children}</span>
        </NavLink>
    );
}
