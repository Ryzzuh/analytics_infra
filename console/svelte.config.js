import adapter from '@sveltejs/adapter-static';

/**
 * Static build, served by Caddy.
 *
 * The Console reads the control plane's API from the browser, so there is nothing for a Node
 * server to do — and a static bundle is one fewer always-on process on a box that is already
 * running thirteen of them.
 */
export default {
	kit: {
		adapter: adapter({ fallback: 'index.html' }),
		prerender: { entries: [] }
	}
};
