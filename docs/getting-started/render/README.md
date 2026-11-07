# Rendering the getting-started GIFs

`render.js` draws each step from the real example rather than from recordings, so the GIFs follow the code. It
reads:
- the files: `examples/library/loan.pure` and the `LoanMachine`, `LoanSpec`, test and `extra.gradle.kts` files in
  `showcases/library`;
- captured output from a real run, in `captured/`.

```bash
node docs/getting-started/render/render.js        # all eight GIFs into docs/getting-started/
```

```bash
node docs/getting-started/render/render.js 4 5    # only steps 4 and 5
```

It needs Playwright with Chromium (`npm i playwright` and `npx playwright install chromium` if they are missing) and
ffmpeg. `page.html` is the frame template: the editor, terminal, Gradle panel and browser views, plus the step
header.

## Re-capturing `captured/`

Re-capture after a change that affects command output:

| file | how it was produced |
|---|---|
| `check.txt` | `./generate-concert-ecosystem check examples/library` |
| `generate.txt` | `./gradlew -q generateEcosystems -Pecosystem=library` on a checkout without `showcases/library` (first run) |
| `LoanMachine.stub.java` | the `LoanMachine.java` that the first run wrote |
| `test.txt`, `it.txt` | `./gradlew --console=plain :library:integrationTest :library:test`, the test lines |
| `deploy.txt` | `./deploy-concert-ecosystem library --no-analytics` |
| `compose-ps.txt` | `docker compose -f docker-compose.yml -f showcases/library/compose.yml ps --format 'table {{.Service}}\t{{.Status}}'` |
| `send-flow.txt` | `./showcases/library/send-flow all` |
| `dashboard-entity.png` | http://localhost:8088/#entity/loan:LOAN-1001 after the flow, full page, 1000 px wide |
