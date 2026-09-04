// Presentation of readiness belongs to the renderer, not the writing model.
const readiness = {
  implemented: { label: 'Работает', tone: 'green' },
  prototype: { label: 'Прототип', tone: 'purple' },
  designed: { label: 'Спроектировано', tone: 'neutral' },
  concept: { label: 'Идея', tone: 'neutral' },
  idea: { label: 'Идея', tone: 'neutral' },
};
export const maturityLabel = (value) => readiness[value?.id] || { label: value?.label || 'Статус уточняется', tone: 'neutral' };
export const basePath = () => import.meta.env.BASE_URL.replace(/\/$/u, '');
export const cardPath = (id) => `${basePath()}/ideas/${id}/`;
export const cardImage = (id) => `${basePath()}/share/cards/${id}.png`;
