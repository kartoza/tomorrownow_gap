
declare global {
  interface Window {
    gtag?: (...args: any[]) => void;
  }
}

export const pageView = (pagePath: string, pageTitle: string): void => {
  if (typeof window.gtag === 'function') {
    const browserLanguage = navigator.language || navigator.userLanguage;
    const userAgent = navigator.userAgent;
    window.gtag('event', 'page_view', {
      page_location: pagePath,
      page_title: pageTitle,
      user_agent: userAgent,
      language: browserLanguage,
    });
  }
}

export const loginEvent = (method?: string): void => {
  if (typeof window.gtag === 'function') {
    window.gtag('event', 'login', {
      method: method || 'website',
    });
  }
}

export const signUpEvent = (method?: string, organization?: string, successful?: boolean): void => {
  if (typeof window.gtag === 'function') {
    window.gtag('event', 'sign_up', {
      method: method || 'website',
      organization: organization || 'Unknown',
      successful: successful !== undefined ? successful : true
    });
  }
}

export const clickEvent = (action: string): void => {
  if (typeof window.gtag === 'function') {
    window.gtag('event', 'button_click', {
      event_category: 'engagement',
      action: action,
    });
  }
}

export const logoutEvent = (): void => {
  if (typeof window.gtag === 'function') {
    window.gtag('event', 'logout', {
      event_category: 'engagement',
      action: 'user_logout',
    });
  }
}

export const trackAndRedirect = (name: string, url: string) => {
  if (typeof window.gtag === 'function') {
    window.gtag?.('event', 'partner_link_click', {
      event_category: 'outbound',
      destination_name: name,
      destination_url: url,
    });
  }

  setTimeout(() => {
    window.open(url, '_blank', 'noopener,noreferrer');
  }, 200);
};

export const trackSectionView = (sectionId: string, pageTitle: string, isFirstView: boolean = false) => {
  if (typeof window.gtag === 'function') {
    window.gtag('event', 'section_view', {
      event_category: 'engagement',
      section_id: sectionId,
      page_title: pageTitle,
      is_first_view: isFirstView,
      page_location: window.location.href
    });
  }
};

export const trackSectionEngagement = (sectionId: string, pageTitle: string, duration: number) => {
  if (typeof window.gtag === 'function') {
    window.gtag('event', 'section_engagement', {
      event_category: 'engagement',
      section_id: sectionId,
      page_title: pageTitle,
      engagement_time_msec: duration,
      engagement_time_sec: Math.round(duration / 1000),
      page_location: window.location.href,
    });
  }
}

export const socialAuthRedirect = (
  provider: 'google' | 'github',
): void => {
  loginEvent(provider);            // analytics
  window.location.href = `/accounts/${provider}/login/`;  // redirect
};
