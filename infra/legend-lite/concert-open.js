// concert's add-on to Legend Studio Lite (level 0): `?concert=<projectId>` opens concert's project read-only
// (#/view/<projectId>). Projects live in this browser, so the first time the project isn't here yet: Studio says it
// can't open it, and this publishes it ("Load demo projects": this site's projects/ are concert's current models plus
// the sinks' Legend store layer, served by the trace UI) and opens it again. Then it opens `&open=<element path>`
// (default: the generated mapping, concert::mapping::Sink). It only drives Studio's own controls, through their
// data-testid / data-path hooks.
(() => {
  const project = new URLSearchParams(location.search).get('concert');
  if (!project) return;
  const view = '#/view/' + encodeURIComponent(project);
  const element = new URLSearchParams(location.search).get('open') || 'concert::mapping::Sink';
  const openElement = async () => {
    const node = await until(() => document.querySelector('[data-testid=explorer] .tree-node[data-path="' + element + '"]'), 30000);
    node.click();
  };
  const until = (pred, ms) => new Promise((resolve, reject) => {
    const t0 = Date.now();
    const iv = setInterval(() => {
      const v = pred();
      if (v) { clearInterval(iv); resolve(v); } else if (Date.now() - t0 > ms) { clearInterval(iv); reject(new Error('timed out')); }
    }, 150);
  });
  const note = (text) => {
    let n = document.getElementById('concert-note');
    if (!n) {
      n = document.createElement('div');
      n.id = 'concert-note';
      n.style.cssText = 'position:fixed;right:16px;bottom:36px;z-index:9999;padding:8px 12px;border-radius:6px;'
        + 'background:#1f6feb;color:#fff;font:13px system-ui;box-shadow:0 2px 8px rgba(0,0,0,.25)';
      document.body.appendChild(n);
    }
    n.textContent = text;
    if (!text) n.remove();
  };
  (async () => {
    try {
      location.hash = view;
      const opened = await until(() => document.querySelector('[data-testid=tabs]') ? 'open'
        : document.querySelector('.fatal') ? 'missing' : null, 60000);
      if (opened === 'open') { await openElement(); return; }
      note('Publishing concert\'s models into this browser…');
      location.hash = '#/';
      (await until(() => document.querySelector('[data-testid=load-demo]'), 30000)).click();
      await until(() => /published/i.test(document.querySelector('[data-testid=demo-status]')?.textContent || ''), 600000);
      note('');
      location.hash = view;
      await until(() => document.querySelector('[data-testid=tabs]'), 60000);
      await openElement();
    } catch (e) {
      note('Could not open ' + project + ': ' + e.message + ' (use "Load demo projects")');
    }
  })();
})();
