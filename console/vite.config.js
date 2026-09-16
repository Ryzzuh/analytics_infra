import { sveltekit } from '@sveltejs/kit/vite';

export default {
	plugins: [sveltekit()],
	server: {
		// In development the API lives on the control plane; in production Caddy serves both
		// from the same origin, so the app only ever uses relative paths.
		proxy: { '/api': 'http://localhost:8006' }
	}
};
