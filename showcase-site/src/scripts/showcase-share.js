(() => {
  const absolute = (value) => new URL(value, window.location.href).href;
  const copiedText = (text) => `${text}\n${window.location.href}`;
  const copy = async (text) => {
    if (!navigator.clipboard || typeof navigator.clipboard.writeText !== 'function') throw new Error('clipboard unavailable');
    await navigator.clipboard.writeText(text);
  };
  document.querySelectorAll('[data-share]').forEach((root) => {
    const title = root.dataset.shareTitle || document.title;
    const text = root.dataset.shareText || title;
    const image = root.dataset.shareImage ? absolute(root.dataset.shareImage) : '';
    const status = root.querySelector('[data-share-status]');
    const say = (message) => { if (status) status.textContent = message; };
    root.querySelector('[data-share-button]')?.addEventListener('click', async () => {
      const data = { title, text, url: window.location.href };
      try {
        if (navigator.share && navigator.canShare && image) {
          const response = await fetch(image);
          if (!response.ok) throw new Error('share image unavailable');
          const blob = await response.blob();
          const file = new File([blob], image.split('/').pop() || 'ideahub-card.svg', { type: blob.type || 'image/svg+xml' });
          if (navigator.canShare({ ...data, files: [file] })) {
            await navigator.share({ ...data, files: [file] });
            return;
          }
        }
        if (navigator.share) {
          await navigator.share(data);
          return;
        }
      } catch (error) {
        if (error?.name === 'AbortError') { say('Поделиться отменено'); return; }
      }
      try { await copy(copiedText(text)); say('Текст и ссылка скопированы'); }
      catch { say('Не удалось скопировать текст и ссылку'); }
    });
    root.querySelector('[data-share-copy]')?.addEventListener('click', async () => {
      try { await copy(copiedText(text)); say('Текст и ссылка скопированы'); }
      catch { say('Не удалось скопировать текст и ссылку'); }
    });
  });
})();
