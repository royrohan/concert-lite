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

// concert's Diagrams panel (Studio Lite has no diagram renderer): a "Diagrams" button opens a drawer with the emitted
// models' diagrams from the trace UI (/concert/diagrams, proxied to /api/model/diagrams): per state machine its
// states (each transition's event and payload class), the classes doing the transitions (the entity root and its
// payload classes) and its whole Pure model; event catalogs' flow and model. The drawer follows the active tab: a
// machine's root or payload class (Pure path a::b::C is the generated Java class ...a.b.C) selects that machine.
(() => {
  let data = null, open = false, selected = null, lastTab = null, mermaidReady = null;
  const css = document.createElement('style');
  css.textContent = `
    #concert-dg-btn{position:fixed;right:16px;bottom:76px;z-index:9998;padding:6px 12px;border-radius:6px;border:0;cursor:pointer;
      background:#1f6feb;color:#fff;font:600 12.5px system-ui;box-shadow:0 2px 8px rgba(0,0,0,.25)}
    #concert-dg-btn.match{background:#d97706}
    #concert-dg{position:fixed;top:0;right:0;bottom:0;width:min(760px,92vw);z-index:9999;display:flex;flex-direction:column;
      background:#fff;color:#1f2328;border-left:1px solid #d0d7de;box-shadow:-4px 0 16px rgba(0,0,0,.15);font:13px system-ui}
    #concert-dg header{display:flex;gap:8px;align-items:center;padding:8px 12px;border-bottom:1px solid #d0d7de}
    #concert-dg header b{margin-right:auto}
    #concert-dg select{font:12.5px ui-monospace,monospace;max-width:320px}
    #concert-dg main{overflow:auto;padding:8px 12px 24px}
    #concert-dg h4{margin:14px 0 6px;font-size:13px} #concert-dg h4 span{font-weight:400;color:#656d76}
    #concert-dg .dg{overflow:auto;border:1px solid #d0d7de;border-radius:6px;padding:8px;background:#fff}
    #concert-dg .kv{color:#656d76;font-size:12px} #concert-dg code{font:12px ui-monospace,monospace;color:#1f2328}
    #concert-dg button.x{border:0;background:none;font-size:18px;cursor:pointer;color:inherit}`;
  document.head.appendChild(css);
  const btn = document.createElement('button');
  btn.id = 'concert-dg-btn';
  btn.textContent = 'Diagrams';
  btn.title = "concert's state machine and model diagrams";
  btn.onclick = () => (open ? close() : show());
  document.body.appendChild(btn);
  const esc = (s) => String(s ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]);

  const load = async () => data || (data = await (await fetch('/concert/diagrams')).json());
  const loadMermaid = () => mermaidReady || (mermaidReady = new Promise((resolve, reject) => {
    const s = document.createElement('script');
    s.src = 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js';
    s.onload = () => { window.mermaid.initialize({ startOnLoad: false, theme: 'default' }); resolve(window.mermaid); };
    s.onerror = () => reject(new Error('could not load mermaid'));
    document.head.appendChild(s);
  }));
  // the machine (or event catalog) whose classes include the Pure element path; a root match wins
  const keyFor = (path) => {
    if (!data || !path || !path.includes('::')) return null;
    const suffix = '.' + path.split('::').join('.');
    const hit = (c) => c === suffix.slice(1) || c.endsWith(suffix);
    const root = data.machines.find((m) => m.root && hit(m.root));
    if (root) return root.smType;
    const any = data.machines.find((m) => m.classes.some(hit));
    if (any) return any.smType;
    const ev = data.eventCatalogs.find((e) => e.classes.some(hit));
    return ev ? 'events:' + ev.name : null;
  };

  function close() { open = false; document.getElementById('concert-dg')?.remove(); }
  async function show(key) {
    open = true;
    let d;
    try { d = await load(); } catch (e) { alert('concert diagrams unavailable: ' + e.message); open = false; return; }
    selected = key || selected || keyFor(lastTab) || d.machines[0]?.smType || (d.eventCatalogs[0] && 'events:' + d.eventCatalogs[0].name);
    let drawer = document.getElementById('concert-dg');
    if (!drawer) {
      drawer = document.createElement('div');
      drawer.id = 'concert-dg';
      document.body.appendChild(drawer);
    }
    const byCatalog = {};
    d.machines.forEach((m) => (byCatalog[m.catalog] = byCatalog[m.catalog] || []).push(m));
    const opt = (k, label) => `<option value="${esc(k)}" ${k === selected ? 'selected' : ''}>${esc(label)}</option>`;
    drawer.innerHTML = `<header><b>Diagrams</b>
        <select id="concert-dg-sel">${Object.entries(byCatalog).map(([c, ms]) => `<optgroup label="${esc(c)}">${ms.map((m) => opt(m.smType, m.smType)).join('')}</optgroup>`).join('')}
          ${d.eventCatalogs.length ? `<optgroup label="event catalogs">${d.eventCatalogs.map((e) => opt('events:' + e.name, e.name)).join('')}</optgroup>` : ''}</select>
        <button class="x" title="Close" id="concert-dg-close">×</button></header><main id="concert-dg-main"></main>`;
    drawer.querySelector('#concert-dg-sel').onchange = (e) => show(e.target.value);
    drawer.querySelector('#concert-dg-close').onclick = close;
    const main = drawer.querySelector('#concert-dg-main');
    const sections = [];
    if (selected?.startsWith('events:')) {
      const e = d.eventCatalogs.find((x) => 'events:' + x.name === selected);
      sections.push(['Event flow', 'event types, what each emits', e?.flowMermaid], ['Model', 'payload and state classes', e?.modelMermaid]);
    } else {
      const m = d.machines.find((x) => x.smType === selected);
      if (m) {
        main.innerHTML = `<div class="kv">catalog <code>${esc(m.catalog)}</code> · entity class <code>${esc(m.root || '(untyped)')}</code>
          · ${m.transitions.length} transitions, ${esc(m.initial)} → ${esc(m.terminal.join(', '))}</div>`;
        sections.push(['State machine', 'each transition: event (payload class)', m.stateMermaid],
          ['Transition classes', 'the entity class and the payload classes that move it', m.transitionMermaid],
          ['Pure model', "every class of the machine's model files", m.modelMermaid]);
      }
    }
    let mm;
    try { mm = await loadMermaid(); } catch (e) { main.insertAdjacentHTML('beforeend', `<p>${esc(e.message)}</p>`); return; }
    for (const [i, [title, note, src]] of sections.entries()) {
      if (!src) continue;
      main.insertAdjacentHTML('beforeend', `<h4>${esc(title)} <span>${esc(note)}</span></h4><div class="dg" id="concert-dg-${i}"></div>`);
      try { document.getElementById('concert-dg-' + i).innerHTML = (await mm.render('cdg' + i + Date.now(), src)).svg; }
      catch (e) { document.getElementById('concert-dg-' + i).textContent = 'diagram: ' + e.message; }
    }
  }

  // follow the active tab: highlight the button when it shows a machine's class, and switch an open drawer to it
  load().catch(() => btn.remove());
  setInterval(() => {
    const tab = document.querySelector('[data-testid=tabs] .tab.active')?.getAttribute('title') || null;
    if (tab === lastTab) return;
    lastTab = tab;
    const key = keyFor(tab);
    btn.classList.toggle('match', !!key);
    btn.title = key ? 'Diagrams of ' + key.replace(/^events:/, 'event catalog ') : "concert's state machine and model diagrams";
    if (open && key && key !== selected) show(key);
  }, 500);
})();
