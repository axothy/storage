import type { Config } from 'tailwindcss';

export default <Config>{
    content: ['./index.html', './src/**/*.{tsx,ts}'],
    darkMode: 'class',
    theme: {
        extend: {
            colors: {
                'bg-main': '#0f0f0f',
            },
        },
    },
    plugins: [],
};
