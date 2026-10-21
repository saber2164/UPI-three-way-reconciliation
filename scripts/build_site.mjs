/**
 * Build the static documentation site.
 *
 * The site's home page is the repository README, and every document under
 * docs/ gets its own page, so the website can never drift from the markdown
 * in the repository.  The output is plain HTML in ./site, which Vercel serves
 * as a static site.
 *
 *   npm install && npm run build
 */
import { marked } from "marked";
import { readFile, writeFile, mkdir, rm, copyFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const out = path.join(root, "site");

const PAGES = [
  { src: "README.md", out: "index.html", title: "Overview" },
  { src: "docs/ARCHITECTURE.md", out: "docs/architecture.html", title: "Architecture" },
  { src: "docs/DECISIONS.md", out: "docs/decisions.html", title: "Decisions" },
  { src: "docs/FAILURE_LOG.md", out: "docs/failure-log.html", title: "Failure log" },
  { src: "docs/OPERATIONS.md", out: "docs/operations.html", title: "Operations" },
  { src: "docs/VERIFICATION.md", out: "docs/verification.html", title: "Verification" },
  { src: "docs/STUDY_GUIDE.md", out: "docs/study-guide.html", title: "Study guide" },
];

const REPO = "https://github.com/saber2164/UPI-three-way-reconciliation";

function nav(current) {
  const items = [
    `<a class="nav-link${current === "dashboard.html" ? " is-active" : ""}" href="/dashboard">Live simulation</a>`,
    ...PAGES.map(
      (p) =>
        `<a class="nav-link${p.out === current ? " is-active" : ""}" href="/${p.out}">${p.title}</a>`,
    ),
  ].join("\n      ");
  return `
    <nav class="nav">
      <a class="brand" href="/">
        <span class="brand-mark">⟕</span>
        <span>UPI three-way reconciliation</span>
      </a>
      <div class="nav-links">
      ${items}
        <a class="nav-link nav-github" href="${REPO}" target="_blank" rel="noreferrer">GitHub</a>
      </div>
    </nav>`;
}

function page({ title, body, current, subtitle }) {
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${title} · UPI three-way reconciliation</title>
<meta name="description" content="Real-time reconciliation of UPI collections across the switch log, the merchant callback and the bank settlement.">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>⟕</text></svg>">
<link rel="stylesheet" href="/assets/style.css">
</head>
<body>
  ${nav(current)}
  <main class="page">
    ${subtitle ? `<p class="kicker">${subtitle}</p>` : ""}
    ${body}
  </main>
  <footer class="footer">
    <p>Generated from the repository &mdash; <a href="${REPO}">saber2164/UPI-three-way-reconciliation</a>.</p>
  </footer>
</body>
</html>
`;
}

async function main() {
  if (existsSync(out)) await rm(out, { recursive: true, force: true });
  await mkdir(path.join(out, "docs"), { recursive: true });
  await mkdir(path.join(out, "assets"), { recursive: true });

  for (const p of PAGES) {
    const md = await readFile(path.join(root, p.src), "utf8");
    const body = await marked.parse(md, { gfm: true, breaks: false });
    const html = page({
      title: p.title,
      subtitle: p.src,
      body,
      current: p.out,
    });
    await mkdir(path.dirname(path.join(out, p.out)), { recursive: true });
    await writeFile(path.join(out, p.out), html, "utf8");
    console.log(`  ${p.src} -> site/${p.out}`);
  }

  await copyFile(
    path.join(root, "scripts", "site.css"),
    path.join(out, "assets", "style.css"),
  );
  console.log("  scripts/site.css -> site/assets/style.css");

  await copyFile(
    path.join(root, "scripts", "dashboard.html"),
    path.join(out, "dashboard.html"),
  );
  console.log("  scripts/dashboard.html -> site/dashboard.html");

  // The published simulation run is embedded in the site, so the dashboard works
  // without any runtime call (and without a CDN cache in the way).  The
  // simulation workflow commits runs/latest.json to main, which rebuilds this.
  const runs = path.join(root, "runs");
  if (existsSync(path.join(runs, "latest.json"))) {
    await mkdir(path.join(out, "runs"), { recursive: true });
    for (const f of ["latest.json", "history.json"]) {
      if (existsSync(path.join(runs, f))) {
        await copyFile(path.join(runs, f), path.join(out, "runs", f));
        console.log(`  runs/${f} -> site/runs/${f}`);
      }
    }
  } else {
    console.log("  no runs/latest.json yet (the simulation workflow publishes it)");
  }

  // A tiny JSON index so the deployment has a machine-readable description.
  await writeFile(
    path.join(out, "site.json"),
    JSON.stringify(
      {
        name: "UPI three-way reconciliation",
        repository: REPO,
        pages: [
          { title: "Live simulation", path: "/dashboard", source: "the simulation workflow" },
          ...PAGES.map((p) => ({ title: p.title, path: "/" + p.out, source: p.src })),
        ],
      },
      null,
      2,
    ),
    "utf8",
  );
  console.log("site built");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
