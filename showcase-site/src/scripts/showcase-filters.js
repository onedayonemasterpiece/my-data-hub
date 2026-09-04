(() => {
  const search = document.querySelector('#showcase-search');
  const filters = [...document.querySelectorAll('[data-showcase-filter]')];
  const cards = [...document.querySelectorAll('[data-showcase-card]')];
  const count = document.querySelector('#showcase-result-count');
  const empty = document.querySelector('#showcase-empty');
  const normalize = (value) => String(value || '').toLocaleLowerCase('ru').trim();
  const update = () => {
    const query = normalize(search?.value);
    let visible = 0;
    for (const card of cards) {
      const filterMatch = filters.every((filter) => {
        const value = filter.value;
        return !value || card.dataset[filter.dataset.showcaseFilter] === value;
      });
      const queryMatch = !query || normalize(card.dataset.search).includes(query);
      const show = filterMatch && queryMatch;
      card.hidden = !show;
      if (show) visible += 1;
    }
    if (count) count.textContent = `${visible} ${visible % 10 === 1 && visible % 100 !== 11 ? 'возможность' : visible % 10 >= 2 && visible % 10 <= 4 && (visible % 100 < 12 || visible % 100 > 14) ? 'возможности' : 'возможностей'}`;
    if (empty) empty.hidden = visible !== 0;
  };
  search?.addEventListener('input', update);
  filters.forEach((filter) => filter.addEventListener('change', update));
  update();
})();

(() => {
  const controls = document.querySelector('[data-search-controls]');
  if (!controls?.querySelector) return;
  const search = controls.querySelector('#showcase-search');
  const panel = controls.querySelector('#showcase-filter-panel');
  const toggle = controls.querySelector('[data-filter-toggle]');
  const filters = [...controls.querySelectorAll('[data-showcase-filter]')];
  const expanded = (value) => {
    if (panel) panel.hidden = !value;
    toggle?.setAttribute('aria-expanded', String(value));
  };
  search?.addEventListener('focus', () => expanded(true));
  toggle?.addEventListener('click', () => expanded(panel?.hidden));
  controls.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') { expanded(false); search?.blur(); }
  });
  controls.addEventListener('focusout', () => {
    setTimeout(() => { if (!controls.contains(document.activeElement)) expanded(false); }, 0);
  });
  filters.forEach((filter) => filter.addEventListener('change', () => {
    const count = filters.filter((item) => item.value).length;
    if (toggle) toggle.textContent = count ? `Фильтры · ${count}` : 'Фильтры';
  }));
  document.querySelectorAll('[data-filter-reset]').forEach((button) => button.addEventListener('click', () => {
    if (search) search.value = '';
    filters.forEach((filter) => { filter.value = ''; });
    if (toggle) toggle.textContent = 'Фильтры';
    search?.dispatchEvent(new Event('input', { bubbles: true }));
    expanded(false);
  }));
})();
