/**
 * The control plane's API.
 *
 * Relative paths only: Caddy serves this bundle and proxies /api to the control plane from the
 * same origin, so there is no base URL to configure and no CORS to arrange.
 */

async function get(path) {
	const response = await fetch(path, { headers: { accept: 'application/json' } });
	if (!response.ok) throw new Error(`${path}: ${response.status}`);
	return response.json();
}

async function post(path, body) {
	const response = await fetch(path, {
		method: 'POST',
		headers: { 'content-type': 'application/json' },
		body: body ? JSON.stringify(body) : undefined
	});
	const payload = await response.json().catch(() => ({}));
	// The API says no in ways worth showing verbatim — "a run is already in progress", a
	// cooldown with a Retry-After — so failures carry their detail rather than a generic error.
	//
	// Not every failure comes from the API, though: a gateway 502 or a passcode challenge
	// returns HTML, and "{}" on screen is worse than useless. Fall back to the status.
	if (!response.ok) {
		const { detail } = payload;
		const text =
			typeof detail === 'string'
				? detail
				: detail
					? JSON.stringify(detail)
					: `${response.status} ${response.statusText || 'request failed'}`;
		throw new Error(text);
	}
	return payload;
}

export const api = {
	summary: () => get('/api/status/summary'),
	incidents: () => get('/api/status/incidents'),
	scenarios: () => get('/api/status/scenarios'),
	runPipeline: () => post('/api/actions/run-pipeline'),
	injectChaos: (key) => post(`/api/actions/chaos/${key}`),
	recoverChaos: (key) => post(`/api/actions/chaos/${key}/recover`),
	reset: () => post('/api/actions/reset', { confirm: 'reset' })
};

export function humaniseSeconds(seconds) {
	if (seconds === null || seconds === undefined) return '—';
	const value = Number(seconds);
	if (value < 90) return `${Math.round(value)}s`;
	if (value < 5400) return `${Math.round(value / 60)}m`;
	if (value < 172800) return `${(value / 3600).toFixed(1)}h`;
	return `${(value / 86400).toFixed(1)}d`;
}

export function sinceText(timestamp) {
	if (!timestamp) return 'never';
	const seconds = (Date.now() - new Date(timestamp).getTime()) / 1000;
	return `${humaniseSeconds(seconds)} ago`;
}
