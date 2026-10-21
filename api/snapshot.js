/**
 * Vercel serverless function: the latest published simulation run.
 *
 * The GitHub Actions workflow runs the reconciliation stack on a runner and
 * publishes `runs/latest.json` (and `runs/history.json`) to the `runs` branch
 * of this repository.  This function serves that document so the dashboard
 * page can show the simulation without any long-lived server and without
 * exposing a database to the internet.
 *
 * The dashboard page prefers the copy embedded in the deployment
 * (`/runs/latest.json`); this endpoint is the fallback and the machine
 * readable form of the same data.
 *
 * It runs on the edge runtime: no cold start, and the fetch happens from the
 * edge next to the reader.  The GitHub *contents* API is used rather than
 * raw.githubusercontent.com, which the edge runtime cannot reach.
 */
export const runtime = "edge";
export const config = { maxDuration: 30 };

const REPO = "saber2164/UPI-three-way-reconciliation";
const BRANCH = "runs";
const CONTENTS = `https://api.github.com/repos/${REPO}/contents`;

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
  "Cache-Control": "s-maxage=60, stale-while-revalidate=300",
};

async function fetchJson(path) {
  const res = await fetch(`${CONTENTS}/${path}?ref=${BRANCH}`, {
    headers: {
      Accept: "application/vnd.github+json",
      "User-Agent": "upi-recon-dashboard",
    },
    signal: AbortSignal.timeout(8000),
    cache: "no-store",
  });
  if (!res.ok) throw new Error(`${path}: HTTP ${res.status}`);
  const body = await res.json();
  const raw = body.content ? atob(body.content.replace(/\n/g, "")) : "";
  return JSON.parse(raw);
}

export default async function handler(request) {
  if (request.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: CORS });
  }

  try {
    const [latest, history] = await Promise.all([
      fetchJson("runs/latest.json"),
      fetchJson("runs/history.json").catch(() => []),
    ]);
    return new Response(JSON.stringify({ latest, history }), {
      status: 200,
      headers: { ...CORS, "Content-Type": "application/json" },
    });
  } catch (err) {
    return new Response(
      JSON.stringify({
        error: "no published simulation run yet",
        detail: String(err && err.message ? err.message : err),
        hint: "the simulation workflow publishes to the 'runs' branch of the repository",
      }),
      { status: 503, headers: { ...CORS, "Content-Type": "application/json" } },
    );
  }
}
