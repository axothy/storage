import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';

import {
    QueryClient,
    QueryClientProvider,
} from '@tanstack/react-query';

import './index.css';

/* ① создаём singleton‑экземпляр */
const queryClient = new QueryClient();

/* ② оборачиваем App в провайдер */
ReactDOM.createRoot(document.getElementById('root')!).render(
    <React.StrictMode>
        <QueryClientProvider client={queryClient}>
            <App />
        </QueryClientProvider>
    </React.StrictMode>
);
