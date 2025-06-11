export const handleSmoothScroll = (href: string) => {
  const targetId = href.substring(1); // Remove the '#'
  const targetElement = document.getElementById(targetId);
  
  if (targetElement) {
    // Different offset for mobile vs desktop
    const isMobile = window.innerWidth < 768;
    const offsetTop = targetElement.offsetTop - (isMobile ? 20 : 80); // No navbar on mobile
    window.scrollTo({
      top: offsetTop,
      behavior: 'smooth'
    });
  }
};