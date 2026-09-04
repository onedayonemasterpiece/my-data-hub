(() => {
  const absolute = (value) => new URL(value, window.location.href).href;
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
          const blob = await fetch(image).then((response) => response.blob());
          const file = new File([blob], image.split('/').pop() || 'ideahub-card.svg', { type: blob.type || 'image/svg+xml' });
          if (navigator.canShare({ ...data, files: [file] })) return await navigator.share({ ...data, files: [file] });
        }
        if (navigator.share) return await navigator.share(data);
        await navigator.clipboard?.writeText(window.location.href); say('Ссылка скопирована');
      } catch (error) { if (error?.name !== 'AbortError') say('Не удалось открыть меню'); }
    });
    root.querySelector('[data-share-copy]')?.addEventListener('click', async () => { await navigator.clipboard?.writeText(window.location.href); say('Ссылка скопирована'); });
  });
})();
