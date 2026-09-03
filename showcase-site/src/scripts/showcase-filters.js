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
    if (count) count.textContent = `${visible} ${visible === 1 ? 'возможность' : 'возможностей'}`;
    if (empty) empty.hidden = visible !== 0;
  };
  search?.addEventListener('input', update);
  filters.forEach((filter) => filter.addEventListener('change', update));
})();
