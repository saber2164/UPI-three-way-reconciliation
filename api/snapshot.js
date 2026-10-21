/**
 * Vercel serverless function: the latest published simulation run.
 *
 * The GitHub Actions workflow runs the reconciliation stack on a runner and
 * publishes `runs/latest.json` (and `runs/history.json`) to the `runs` branch
 * of this repository.  This function serves that document so the dashboard
 * page can show the simulation without any long-lived server and without
 * exposing a database to the internet.
 */
const REPO = "saber2164/UPI-three-way-reconciliation";
const BRANCH = "runs";
// The explicit "refs/heads/<branch>" form is required: with the short form
// raw.githubusercontent.com would read "runs" as the file path.
const RAW = `https://raw.githubusercontent.com/${REPO}/refs/heads/${BRANCH}/runs`;

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
  "Cache-Control": "s-maxage=60, stale-while-revalidate=300",
};

async function fetchJson(name) {
  const res = await fetch(`${RAW}/${name}`, {
    headers: { "User-Agent": "upi-recon-dashboard" },
  });
  if (!res.ok) throw new Error(`${name}: ${res.status}`);
  return res.json();
}

export default async function handler(request) {
  if (request.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: CORS });
  }

  try {
    const [latest, history] = await Promise.all([
      fetchJson("latest.json"),
      fetchJson("history.json").catch(() => []),
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
