import { trackAndRedirect } from './analytics';

export const openInNewTab = (url: string, name?: string) => {
  trackAndRedirect(name || 'external_link', url);
};
