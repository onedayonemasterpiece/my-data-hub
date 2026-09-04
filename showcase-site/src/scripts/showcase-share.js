// Only explicit clicks share/copy. Local interest never sends an event or request.
(() => {
  const absolute = (value) => new URL(value, window.location.href).href;
  const copiedText = (text, url) => `${text}\n${url}`;
  const copy = async (text) => {
    if (!navigator.clipboard?.writeText) throw new Error('clipboard unavailable');
    await navigator.clipboard.writeText(text);
  };
  document.querySelectorAll('[data-share]').forEach((root) => {
    const title = root.dataset.shareTitle || document.title;
    const text = root.dataset.shareText || title;
    const url = absolute(root.dataset.shareUrl || window.location.href);
    const image = root.dataset.shareImage ? absolute(root.dataset.shareImage) : '';
    const status = root.querySelector('[data-share-status]');
    const say = (message) => { if (status) status.textContent = message; };
    const fallback = async () => {
      try { await copy(copiedText(text, url)); say('Текст и ссылка скопированы'); }
      catch {
        say('Не удалось скопировать текст и ссылку автоматически. Можно скопировать их из поля ниже.');
        const field = root.querySelector('[data-share-fallback]');
        const area = field?.querySelector('textarea');
        if (field && area) { field.hidden = false; area.value = copiedText(text, url); area.focus(); area.select(); }
      }
    };
    // A prepared file preserves browser user activation on native share.
    // Only nearby cards are prefetched, and never a third-party image or URL.
    let preparedFile;
    let loading = false;
    const prepare = async () => {
      if (loading || !navigator.canShare || !navigator.share || !image) return;
      if (new URL(image).origin !== window.location.origin) return;
      loading = true;
      try {
        const response = await fetch(image, { credentials: 'same-origin' });
        if (!response.ok) return;
        const blob = await response.blob();
        if (blob.type !== 'image/png' || blob.size > 2_000_000) return;
        preparedFile = new File([blob], 'ideahub-card.png', { type: 'image/png' });
      } catch { /* Link sharing remains available without an image. */ }
    };
    root.addEventListener('pointerenter', prepare, { once: true });
    root.addEventListener('focusin', prepare, { once: true });
    if ('IntersectionObserver' in window && navigator.share) {
      const observer = new IntersectionObserver((entries) => {
        if (entries.some((entry) => entry.isIntersecting)) { prepare(); observer.disconnect(); }
      }, { rootMargin: '120px' });
      observer.observe(root);
    }
    root.querySelector('[data-share-button]')?.addEventListener('click', async () => {
      const data = { title, text, url };
      if (navigator.share) {
        try {
          const file = preparedFile;
          if (file && navigator.canShare?.({ ...data, files: [file] })) {
            await navigator.share({ ...data, files: [file] });
          } else {
            await navigator.share(data);
          }
          say('Открыто меню отправки');
          return;
        } catch (error) {
          if (error?.name === 'AbortError') { say('Поделиться отменено'); return; }
        }
      }
      await fallback();
    });
    root.querySelector('[data-share-copy]')?.addEventListener('click', fallback);
  });

  const catalog = document.querySelector('[data-interest-catalog]');
  const viewId = document.body.dataset.showcaseView;
  if (!catalog || !viewId) return;
  const items = new Map([...catalog.querySelectorAll('[data-item-id]')].map((node) => [node.dataset.itemId, node]));
  const key = `ideahub-interest:${viewId}`;
  let selected = new Set();
  let persistent = true;
  try {
    const value = JSON.parse(localStorage.getItem(key) || '[]');
    if (Array.isArray(value)) selected = new Set(value.filter((id) => items.has(id)));
  } catch { persistent = false; }
  const section = document.querySelector('[data-interest-message]');
  const area = document.querySelector('[data-interest-text]');
  const status = document.querySelector('[data-interest-status]');
  const update = () => {
    document.querySelectorAll('[data-interest-id]').forEach((button) => {
      const on = selected.has(button.dataset.interestId);
      button.setAttribute('aria-pressed', String(on));
      const mark = button.querySelector('[data-interest-mark]');
      if (mark) mark.textContent = on ? '★' : '☆';
    });
    if (section) section.hidden = selected.size === 0;
    const count = document.querySelector('[data-interest-count]');
    if (count) count.textContent = `(${selected.size})`;
    if (area) area.value = `Здравствуйте! Меня заинтересовали сценарии из подборки «${document.body.dataset.showcaseTitle || ''}»:\n\n` +
      [...items].filter(([id]) => selected.has(id)).map(([, node]) => `#${node.dataset.itemNumber} — ${node.textContent}\n${node.href}`).join('\n\n');
    if (status) status.textContent = persistent ? '' : 'Хранение в браузере недоступно: отметки сохранятся только до закрытия страницы.';
  };
  document.querySelectorAll('[data-interest-id]').forEach((button) => button.addEventListener('click', () => {
    const id = button.dataset.interestId;
    if (!items.has(id)) return;
    if (selected.has(id)) selected.delete(id); else selected.add(id);
    try { localStorage.setItem(key, JSON.stringify([...selected])); } catch { persistent = false; }
    update();
  }));
  document.querySelector('[data-interest-copy]')?.addEventListener('click', async () => {
    try { await copy(area?.value || ''); if (status) status.textContent = 'Сообщение скопировано. Откройте контакт и отправьте его.'; }
    catch { area?.focus(); area?.select(); if (status) status.textContent = 'Скопируйте сообщение из поля выше.'; }
  });
  window.addEventListener('storage', (event) => {
    if (event.key !== key) return;
    try { const value = JSON.parse(event.newValue || '[]'); selected = new Set(Array.isArray(value) ? value.filter((id) => items.has(id)) : []); update(); }
    catch { /* Ignore malformed external storage changes. */ }
  });
  update();
})();
