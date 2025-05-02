// src/hooks/useApi.ts
import axios from 'axios';

const api = axios.create({ baseURL: '/api' });

export async function getConfig() {
    const { data } = await api.get('/config');
    return data;
}
export async function patchConfig(payload: any) {
    await api.patch('/config', payload);
}

export async function forceFlush() {
    await api.post('/flush');
}
export async function forceCompact() {
    await api.post('/compact');
}

export async function getNodes() {
    const { data } = await api.get('/nodes');
    return data; // [{id, role, lastHeartbeatMs, address}]
}

export async function scan(from: string, to: string) {
    const { data } = await api.get('/entries', { params: { from, to }});
    return data; // массив ключ/значение
}
