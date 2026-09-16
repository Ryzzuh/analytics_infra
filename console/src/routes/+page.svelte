<script>
	import { onMount, onDestroy } from 'svelte';
	import { api, humaniseSeconds, sinceText } from '$lib/api.js';

	let summary = $state(null);
	let incidents = $state({ available: false, alerts: [] });
	let scenarios = $state([]);
	let error = $state(null);
	let busy = $state(null);
	let message = $state(null);
	let timer;

	async function refresh() {
		try {
			const [s, i] = await Promise.all([api.summary(), api.incidents()]);
			summary = s;
			incidents = i;
			error = null;
		} catch (e) {
			// The page keeps showing the last good state rather than blanking: a control plane
			// restart should not look like the platform disappeared.
			error = e.message;
		}
	}

	onMount(async () => {
		scenarios = (await api.scenarios()).scenarios;
		await refresh();
		timer = setInterval(refresh, 10000);
	});

	onDestroy(() => clearInterval(timer));

	async function act(label, fn) {
		busy = label;
		message = null;
		try {
			const result = await fn();
			message = { kind: 'ok', text: describe(label, result) };
		} catch (e) {
			// Refusals are shown as-is: "a run is already in progress" is more useful than
			// "something went wrong", and it is the server's decision being explained.
			message = { kind: 'refused', text: e.message };
		} finally {
			busy = null;
			await refresh();
		}
	}

	function describe(label, result) {
		if (result.triggered) return `Pipeline run ${result.triggered} started.`;
		if (result.recovered) return `${label}: recovered.`;
		if (result.window_id) return `${label}: injected. Watch the symptoms below.`;
		if (result.reset) return 'Reset started. Restore plus catch-up takes a few minutes.';
		return `${label}: done.`;
	}

	const openScenarios = $derived(new Set((summary?.chaos_windows ?? []).map((w) => w.scenario)));
	const failingChecks = $derived((summary?.checks ?? []).filter((c) => c.status === 'fail'));
	const blockingDrift = $derived((summary?.drift ?? []).filter((d) => d.blocking));

	function freshnessState(row) {
		if (row.age_seconds === null) return 'bad';
		return row.age_seconds > row.target_seconds ? 'bad' : 'good';
	}
</script>

<svelte:head><title>Platform Console</title></svelte:head>

<main>
	<header>
		<div>
			<h1>Analytics platform</h1>
			<p class="sub">
				Live status. Everything here is read from the platform itself — freshness from the
				warehouse, alerts from Prometheus.
			</p>
		</div>
		<div class="updated">
			{#if summary}updated {sinceText(summary.generated_at)}{/if}
			{#if error}<span class="stale">stale: {error}</span>{/if}
		</div>
	</header>

	{#if summary?.paging_suppressed}
		<!-- Said out loud, because otherwise a reviewer sees alerts firing, nobody paged, and no
		     explanation for the difference. -->
		<div class="banner">
			<strong>Chaos scenario in progress — paging is suppressed.</strong>
			Alerts still fire and still appear below; they are not being sent to anyone.
			{#each summary.chaos_windows as w}
				<span class="pill">{w.scenario} · {sinceText(w.opened_at)}</span>
			{/each}
		</div>
	{/if}

	{#if message}
		<div class="message {message.kind}">{message.text}</div>
	{/if}

	<section class="actions">
		<button disabled={busy} onclick={() => act('Run pipeline', api.runPipeline)}>
			{busy === 'Run pipeline' ? 'Starting…' : 'Run pipeline'}
		</button>
		<button
			class="danger"
			disabled={busy}
			onclick={() => confirm('Reset discards everything since the last snapshot. Continue?') && act('Reset', api.reset)}
		>
			Reset to known-good
		</button>
		<span class="hint">Actions need the demo passcode; the status above is public.</span>
	</section>

	<div class="grid">
		<section>
			<h2>Freshness <span class="count">against SLO</span></h2>
			<table>
				<thead><tr><th>Layer</th><th>Age</th><th>SLO</th><th></th></tr></thead>
				<tbody>
					{#each summary?.freshness ?? [] as row}
						<tr>
							<td>{row.layer}</td>
							<td>{humaniseSeconds(row.age_seconds)}</td>
							<td class="muted">{humaniseSeconds(row.target_seconds)}</td>
							<td><span class="dot {freshnessState(row)}"></span></td>
						</tr>
					{:else}
						<tr><td colspan="4" class="muted">No checks have run yet.</td></tr>
					{/each}
				</tbody>
			</table>
		</section>

		<section>
			<h2>Ingestion <span class="count">last hour</span></h2>
			<table>
				<thead><tr><th>Topic</th><th>Rows</th><th>DLQ 24h</th><th>Last load</th></tr></thead>
				<tbody>
					{#each summary?.loads ?? [] as row}
						<tr>
							<td class="mono">{row.topic}</td>
							<td>{row.rows_last_hour ?? 0}</td>
							<td class:warn={row.dlq_last_day > 0}>{row.dlq_last_day ?? 0}</td>
							<td class="muted">{sinceText(row.last_load_at)}</td>
						</tr>
					{:else}
						<tr><td colspan="4" class="muted">Nothing loaded yet.</td></tr>
					{/each}
				</tbody>
			</table>
		</section>

		<section>
			<h2>
				Incidents
				<span class="count">{incidents.available ? `${incidents.alerts.length} firing` : 'unavailable'}</span>
			</h2>
			{#each incidents.alerts as alert}
				<article class="incident {alert.severity}">
					<div class="row">
						<strong>{alert.name}</strong>
						<span class="muted">{sinceText(alert.since)}</span>
					</div>
					<p>{alert.summary}</p>
					{#if alert.runbook}<a href="https://github.com/Ryzzuh/analytics_infra/blob/main/{alert.runbook}">runbook</a>{/if}
				</article>
			{:else}
				<p class="muted">
					{incidents.available ? 'Nothing firing.' : 'Prometheus is unreachable — the rest of this page is unaffected.'}
				</p>
			{/each}
		</section>

		<section>
			<h2>Data quality <span class="count">{failingChecks.length} failing</span></h2>
			<table>
				<thead><tr><th>Check</th><th>Observed</th><th>Threshold</th><th></th></tr></thead>
				<tbody>
					{#each summary?.checks ?? [] as check}
						<tr>
							<td>{check.check_name}<br /><span class="muted mono">{check.target}</span></td>
							<td>{check.observed === null ? '—' : Number(check.observed).toFixed(3)}</td>
							<td class="muted">{check.threshold === null ? '—' : Number(check.threshold).toFixed(3)}</td>
							<td><span class="dot {check.status === 'pass' ? 'good' : check.status === 'warn' ? 'warn-dot' : 'bad'}"></span></td>
						</tr>
					{:else}
						<tr><td colspan="4" class="muted">No checks have run yet.</td></tr>
					{/each}
				</tbody>
			</table>
			{#if blockingDrift.length}
				<p class="drift">
					<strong>{blockingDrift.length} model(s) blocked by schema drift:</strong>
					{#each blockingDrift as d}<span class="pill">{d.event_type}.{d.json_path} {d.change}</span>{/each}
				</p>
			{/if}
		</section>
	</div>

	<section>
		<h2>Chaos scenarios</h2>
		<p class="sub">
			Each one breaks something real and recovers it. Nothing here fakes a symptom: the
			platform is genuinely reacting.
		</p>
		<div class="scenarios">
			{#each scenarios as scenario}
				{@const running = openScenarios.has(scenario.key)}
				<article class="scenario" class:running>
					<h3>{scenario.title}</h3>
					<p>{scenario.what_breaks}</p>
					<details>
						<summary>What should happen</summary>
						<ul>
							{#each scenario.expected_symptoms as symptom}<li>{symptom}</li>{/each}
						</ul>
						<p class="muted">{scenario.recovery}</p>
					</details>
					<div class="row">
						{#if running}
							<button disabled={busy} onclick={() => act(scenario.title, () => api.recoverChaos(scenario.key))}>
								Recover
							</button>
							<span class="running-label">running</span>
						{:else}
							<button disabled={busy} onclick={() => act(scenario.title, () => api.injectChaos(scenario.key))}>
								Inject
							</button>
						{/if}
						<a class="muted" href="https://github.com/Ryzzuh/analytics_infra/blob/main/{scenario.runbook}">runbook</a>
					</div>
				</article>
			{/each}
		</div>
	</section>
</main>

<style>
	:global(body) {
		margin: 0;
		font-family: ui-sans-serif, system-ui, -apple-system, 'Segoe UI', sans-serif;
		color: #18181b;
		background: #fafafa;
	}

	main { max-width: 1100px; margin: 0 auto; padding: 2rem 1.25rem 4rem; }

	header { display: flex; justify-content: space-between; align-items: flex-start; gap: 1rem; }
	h1 { font-size: 1.5rem; margin: 0 0 0.25rem; }
	h2 { font-size: 0.95rem; text-transform: uppercase; letter-spacing: 0.04em; color: #52525b; margin: 0 0 0.75rem; }
	h3 { font-size: 1rem; margin: 0 0 0.35rem; }
	.sub { color: #71717a; font-size: 0.875rem; margin: 0; max-width: 60ch; }
	.count { text-transform: none; letter-spacing: 0; color: #a1a1aa; font-weight: 400; }
	.updated { font-size: 0.8rem; color: #a1a1aa; text-align: right; }
	.stale { display: block; color: #b45309; }

	.banner {
		margin: 1.25rem 0; padding: 0.75rem 1rem; border-radius: 8px;
		background: #fef3c7; border: 1px solid #fcd34d; font-size: 0.875rem;
	}

	.message { margin: 1rem 0; padding: 0.7rem 1rem; border-radius: 8px; font-size: 0.875rem; }
	.message.ok { background: #dcfce7; border: 1px solid #86efac; }
	.message.refused { background: #fee2e2; border: 1px solid #fca5a5; }

	.actions { display: flex; align-items: center; gap: 0.75rem; margin: 1.5rem 0; }
	.hint { color: #a1a1aa; font-size: 0.8rem; }

	button {
		font: inherit; font-size: 0.875rem; padding: 0.45rem 0.9rem; border-radius: 6px;
		border: 1px solid #d4d4d8; background: white; cursor: pointer;
	}
	button:hover:not(:disabled) { border-color: #a1a1aa; }
	button:disabled { opacity: 0.5; cursor: default; }
	button.danger { border-color: #fca5a5; color: #b91c1c; }

	.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(340px, 1fr)); gap: 1.25rem; }
	section { background: white; border: 1px solid #e4e4e7; border-radius: 10px; padding: 1.1rem 1.25rem; margin-bottom: 1.25rem; }

	table { width: 100%; border-collapse: collapse; font-size: 0.875rem; }
	th { text-align: left; font-weight: 500; color: #a1a1aa; font-size: 0.75rem; padding-bottom: 0.4rem; }
	td { padding: 0.4rem 0; border-top: 1px solid #f4f4f5; vertical-align: top; }
	.mono { font-family: ui-monospace, 'SF Mono', monospace; font-size: 0.8rem; }
	.muted { color: #a1a1aa; }
	.warn { color: #b45309; font-weight: 600; }

	.dot { display: inline-block; width: 9px; height: 9px; border-radius: 50%; }
	.dot.good { background: #22c55e; }
	.dot.warn-dot { background: #f59e0b; }
	.dot.bad { background: #ef4444; }

	.incident { border-left: 3px solid #f59e0b; padding: 0.4rem 0 0.4rem 0.75rem; margin-bottom: 0.75rem; font-size: 0.875rem; }
	.incident.critical { border-color: #ef4444; }
	.incident p { margin: 0.2rem 0; }
	.row { display: flex; align-items: center; gap: 0.75rem; justify-content: space-between; }

	.pill { display: inline-block; background: #f4f4f5; border-radius: 999px; padding: 0.1rem 0.6rem; font-size: 0.75rem; margin-left: 0.4rem; }
	.drift { font-size: 0.8rem; margin: 0.75rem 0 0; }

	.scenarios { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 1rem; margin-top: 1rem; }
	.scenario { border: 1px solid #e4e4e7; border-radius: 8px; padding: 0.9rem 1rem; font-size: 0.875rem; }
	.scenario.running { border-color: #fcd34d; background: #fffbeb; }
	.scenario p { color: #52525b; margin: 0 0 0.6rem; }
	.scenario ul { margin: 0.4rem 0; padding-left: 1.1rem; color: #52525b; }
	.scenario .row { justify-content: flex-start; margin-top: 0.7rem; }
	details summary { cursor: pointer; color: #52525b; font-size: 0.8rem; }
	.running-label { color: #b45309; font-size: 0.8rem; }

	a { color: #2563eb; font-size: 0.8rem; }
</style>
